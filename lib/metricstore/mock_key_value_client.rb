module Metricstore
  # Internal class. Use this class outside the gem at your own risk.
  # TTL is ignored. Not thread-safe. For testing purposes only.
  class MockKeyValueClient
    
    def initialize(*args, &callback)
      @store = {}
    end

    def increment(key, delta, opts={})
      if @store.include?(key)
        @store[key] += delta
      else
        @store[key] = delta
      end
      [@store[key], @store[key]]
    end

    def add(key, value, opts={})
      return nil if @store.include?(key)
      @store[key] = value
      [value, value]
    end

    def set(key, value, opts={})
      return nil if opts[:cas] && opts[:cas] != @store[key]
      @store[key] = value
    end

    def fetch(key, opts={})
      value = @store[key]
      value.nil? ? nil : [value, value]
    end

    def to_s
      "MockKeyValueClient: #{@store.inspect}"
    end
  end
end
