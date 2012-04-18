require 'couchbase'

module Metricstore
  # Internal class. Use this class outside the gem at your own risk.
  class CouchbaseClient
    
    def initialize(*args, &callback)
      super
      @connection = Couchbase.connect(*args, &callback)
    end

    # key: a string
    # delta: an integer
    # options:
    #  :ttl => Time-to-live (number of seconds from now).
    # returns: [value, cas_version_id]
    def increment(key, delta, opts={})
      options = {:initial => delta, :extended => true}
      options.merge(:ttl => convert_ttl(opts[:ttl])) if opts.include?(:ttl)
      value, flags, cas = connection.incr(key, delta, options)
      [value, cas]
    end

    # key: a string
    # delta: a marshalable object
    # options:
    #  :ttl => Time-to-live (number of seconds from now).
    # returns: cas_version_id, or nil if the key already exists. 
    def add(key, value, opts={})
      options = {}
      options.merge(:ttl => convert_ttl(opts[:ttl])) if opts.include?(:ttl)
      connection.add(key, value, opts)
    rescue Couchbase::Error::KeyExists => e
      nil
    end

    # key: a string
    # value: a marshalable object
    # options:
    #  :ttl => Time-to-live (number of seconds from now).
    #  :cas => a version id (for optimistic concurrency control)
    # returns: cas_version_id, or nil if the key already exists. 
    def set(key, value, opts={})
      options = {}
      options.merge(:ttl => convert_ttl(opts[:ttl])) if opts.include?(:ttl)
      options.merge(:cas => opts[:cas]) if opts.include?(:cas)
      connection.set(key, value, opts)
    rescue Couchbase::Error::KeyExists => e
      nil
    end

    # key: a string
    # returns: [value, cas_version_id], or nil if the key doesn't exist.
    def fetch(key, opts={})
      options = {:extended => true, :quiet => true}
      options.merge(:ttl => convert_ttl(opts[:ttl])) if opts.include?(:ttl)
      value, flags, cas = connection.get(key, options)
      value.nil? ? nil : [value, cas]
    end

    private

    attr_reader :connection

    def convert_ttl(ttl)
      ttl.nil? ? nil : ttl.to_f <= 0 ? nil : (Time.now + ttl.to_f).to_f
    end

  end
end