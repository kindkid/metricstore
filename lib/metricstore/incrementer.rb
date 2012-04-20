module Metricstore
  class Incrementer < Updater

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
      stored_value, cas = kvstore.fetch(key, :ttl => ttl)
      if stored_value.nil?
        if kvstore.add(key, delta, :ttl => ttl)
          return delta
        else
          # collision
          retry_update(key, delta, ttl, errors)
          return nil
        end
      else
        new_value = stored_value + delta
        if kvstore.set(key, new_value, :ttl => ttl, :cas => cas)
          return new_value
        else
          # collision
          retry_update(key, min_max, ttl, errors)
          return nil
        end
      end
    end
  end
end