module Metricstore
  class Incrementer < Updater

    def increment(key, delta, ttl=nil)
      update(key, delta, ttl)
    end

    protected

    def prepare_data(delta)
      delta
    end
    
    def consolidate_data(delta1, delta2)
      delta1 + delta2
    end

    def handle_update(key, delta, ttl, errors)
      kvstore.increment(key, delta, :ttl => ttl)
    end
  end
end