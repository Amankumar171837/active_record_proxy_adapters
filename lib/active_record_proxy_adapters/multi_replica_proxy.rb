# lib/active_record_proxy_adapters/multi_replica_proxy.rb
# frozen_string_literal: true

require "active_record_proxy_adapters/primary_replica_proxy"

module ActiveRecordProxyAdapters
  # MultiReplicaProxy extends the PrimaryReplicaProxy behavior to support
  # multiple read-replicas with a simple round-robin selector.
  class MultiReplicaProxy < PrimaryReplicaProxy
    # A simple manager object that acts like a single "pool" (as expected by
    # PrimaryReplicaProxy). It holds many real pools and dispatches checkouts
    # in a round-robin fashion.
    class ReplicaPoolCollection
      def initialize(pools)
        @pools = pools.compact
        @mutex = Mutex.new
        @index = 0
        # mapping of connection.object_id => pool (so checkin knows which pool)
        @checked_out = {}
      end

      # behave like a pool: checkout should return a connection (or raise)
      def checkout(timeout = nil)
        raise ActiveRecord::ConnectionNotEstablished, "no replica pools available" if @pools.empty?

        pool = next_pool
        conn = pool.checkout(timeout)
        @mutex.synchronize { @checked_out[conn.object_id] = pool }
        conn
      rescue StandardError => e
        # bubble up; caller will fallback to primary
        raise
      end

      # checkin a connection back to the correct underlying pool
      def checkin(connection)
        return unless connection

        pool = nil
        @mutex.synchronize { pool = @checked_out.delete(connection.object_id) }
        # If we didn't find a mapping, try to best-effort find the owner pool.
        pool ||= @pools.find { |p| p.owned_connection?(connection) rescue false }

        if pool
          pool.checkin(connection)
        else
          # best-effort: disconnect if we cannot find owner
          connection.disconnect! if connection.respond_to?(:disconnect!)
        end
      end

      # Returns true if there are no pools available
      def empty?
        @pools.empty?
      end

      private

      # Round-robin selection (thread-safe)
      def next_pool
        @mutex.synchronize do
          pool = @pools[@index % @pools.size]
          @index = (@index + 1) % @pools.size
          pool
        end
      end
    end

    # Override initialize to build the replica collection (lazy)
    def initialize(primary_connection)
      super(primary_connection)
      @replica_collection_mutex = Mutex.new
      @replica_collection = nil
    end

    private

    # PrimaryReplicaProxy used `replica_pool` expecting a single object with
    # checkout/checkin. We override replica_pool to return our collection.
    def replica_pool
      @replica_collection ||= build_replica_collection
    end

    def replica_pool_unavailable?
      replica_pool.nil? || replica_pool.empty?
    end

    # Attempt to discover all replica pools for this app environment.
    # This method tries a few approaches to find all replica pools:
    #  1. If ActiveRecord::Base.configurations supports `configs_for`, we will
    #     search for entries with `replica: true`.
    #  2. As a fallback, we try connection_handler.retrieve_connection_pool for
    #     any named replica pools we can infer.
    #
    # You may need to adapt the discovery logic for your Rails / AR version.
    # def build_replica_collection
    #   pools = []
    #
    #   # 1) Inspect database configurations for replica: true entries
    #   begin
    #     if defined?(ActiveRecord::Base.configurations) &&
    #         ActiveRecord::Base.configurations.respond_to?(:configs_for)
    #       configs = ActiveRecord::Base.configurations.configurations.select { |c| c.env_name == Rails.env }
    #       configs.each do |db_config|
    #         begin
    #           # Many db_config objects respond to `replica?` or have `replica` flag in config hash
    #           replica_flag = db_config.respond_to?(:replica?) ? db_config.replica? : db_config.configuration_hash&.[](:replica)
    #           next unless replica_flag
    #
    #           puts "===dbconfigname====#{db_config.name.inspect}==="
    #
    #           pool = connection_handler.retrieve_connection_pool(db_config.name, role: reading_role) rescue nil
    #           puts "=====pool====#{pool.inspect}==="
    #           puts "======pools====#{pools.inspect}==="
    #           pools << pool if pool
    #         rescue StandardError
    #           next
    #         end
    #       end
    #     end
    #   rescue StandardError
    #     # ignore discovery errors, we'll try other ways below
    #   end
    #
    #   # 2) Fallback: try to use the default replica pool the original proxy used
    #   begin
    #     specific = specific_replica_pool
    #     pools << specific if specific
    #     default = default_replica_pool
    #     pools << default if default && !pools.include?(default)
    #   rescue StandardError
    #     # ignore
    #   end
    #
    #   # Ensure uniqueness and compact
    #   pools = pools.compact.uniq
    #
    #   # If nothing found, return nil so the parent falls back to primary
    #   return nil if pools.empty?
    #
    #   ReplicaPoolCollection.new(pools)
    # end


    # def build_replica_collection
    #   puts "🔍 [Replica Debug] Starting build_replica_collection..."
    #   pools = []
    #
    #   # 1) Find DB configurations for current environment
    #   if defined?(ActiveRecord::Base.configurations)
    #     configs = ActiveRecord::Base.configurations.configurations.select { |c| c.env_name == Rails.env }
    #     puts "✅ [Replica Debug] Total DB configs in #{Rails.env}: #{configs.map(&:name)}"
    #
    #     configs.each do |db_config|
    #       begin
    #         replica_flag =
    #             if db_config.respond_to?(:replica?)
    #               db_config.replica?
    #             else
    #               db_config.configuration_hash[:replica]
    #             end
    #
    #         puts "➡️  Checking config: #{db_config.name} | replica?: #{replica_flag.inspect}"
    #
    #         next unless replica_flag
    #
    #         # Try retrieving an open pool for this DB
    #         pool = connection_handler.retrieve_connection_pool(db_config.name, role: reading_role) rescue nil
    #         if pool
    #           puts "✅ [Replica Debug] Found pool for #{db_config.name} => #{pool_db_config_name(pool)}"
    #         else
    #           puts "❌ [Replica Debug] No pool found for replica '#{db_config.name}' (maybe not initialized by connects_to?)"
    #         end
    #
    #         pools << pool if pool
    #       rescue => e
    #         puts "⚠️ [Replica Debug] Error checking #{db_config.name}: #{e.message}"
    #         next
    #       end
    #     end
    #   end
    #
    #   # 2) Fallback options
    #   begin
    #     specific = specific_replica_pool
    #     puts "ℹ️  Fallback specific_replica_pool => #{pool_db_config_name(specific)}"
    #     pools << specific if specific
    #
    #     default = default_replica_pool
    #     puts "ℹ️  Fallback default_replica_pool => #{pool_db_config_name(default)}"
    #     pools << default if default && !pools.include?(default)
    #   rescue => e
    #     puts "⚠️  [Replica Debug] Fallback check failed: #{e.message}"
    #   end
    #
    #   pools = pools.compact.uniq
    #   puts "🔚 [Replica Debug] Final replica pools: #{pools.map { |p| pool_db_config_name(p) }}"
    #
    #   return nil if pools.empty?
    #   ReplicaPoolCollection.new(pools)
    # end
    #
    #

    def build_replica_collection
      puts "🔍 [Replica Debug] Starting build_replica_collection..."

      pools = []

      # ✅ Directly pick pools by replica names
      [:replica_1, :replica_2].each do |replica_name|
        pool = connection_handler.retrieve_connection_pool(replica_name, role: reading_role) rescue nil
        if pool
          puts "✅ Found pool for #{replica_name} => #{pool_db_config_name(pool)}"
          pools << pool
        else
          puts "❌ No pool for #{replica_name} (not initialized yet)"
        end
      end

      # Remove nils + duplicates
      pools = pools.compact.uniq

      puts "🔚 [Replica Debug] Final replica pools: #{pools.map { |p| pool_db_config_name(p) }}"

      return nil if pools.empty?
      ReplicaPoolCollection.new(pools)
    end


    # Helper method to print pool name safely
    def pool_db_config_name(pool)
      pool && pool.db_config ? pool.db_config.name : "nil"
    end

    # checkout_replica_connection is inherited, but we keep the fallback behavior:
    # if checkout raises (or there are no replica pools), parent will return primary.
    def checkout_replica_connection
      replica_pool.checkout(proxy_checkout_timeout)
    rescue ActiveRecord::NoDatabaseError, ActiveRecord::ConnectionNotEstablished, StandardError
      # same fallback behavior as original: use primary when replicas fail
      primary_connection
    end
  end
end
