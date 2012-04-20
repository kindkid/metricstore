module Metricstore
  class CountIncrementer < Updater

    def increment(key, delta, ttl=nil)
      return if delta.zero?
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