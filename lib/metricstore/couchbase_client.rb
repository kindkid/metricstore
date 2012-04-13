module Metricstore
  class CouchbaseClient
    include BaseClient
    
    def initialize(*args, &callback)
      super

      require 'couchbase'
      @couchbase = Couchbase.connect(*args, &callback)
    end

    private

    attr_reader :couchbase
  end
end