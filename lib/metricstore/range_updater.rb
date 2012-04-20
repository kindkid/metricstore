module Metricstore
  class RangeUpdater < Updater

    def update_range(key, value, ttl=nil)
      raise(ArgumentError, "value must be numeric") unless value.is_a?(Numeric)
      update(key, [value, value], ttl)
    end

    protected

    def prepare_data(min_max)
      min_max
    end

    def consolidate_data(min_max1, min_max2)
      [min(min_max1[0], min_max2[0]), max(min_max1[1], min_max2[1])]
    end

    # Returns nil if there was contention, and we have to retry.
    # Returns [:new, range] where range is (max - min), if range was added.
    # Otherwise returns [:grew, diff] where diff is the amount the range grew.
    def handle_update(key, min_max, ttl, errors)
      #TODO: there's room here for a local cache optimization
      stored_min_max, cas = kvstore.fetch(key, :ttl => ttl)
      if stored_min_max.nil?
        if kvstore.add(key, min_max, :ttl => ttl)
          return [:new, (min_max[1] - min_max[0])]
        else
          # collision
          retry_update(key, min_max, ttl, errors)
          return nil
        end
      else
        stored_min, stored_max = stored_min_max
        new_min = min(stored_min, min_max[0])
        new_max = max(stored_max, min_max[1])
        return 0 if new_min == stored_min && new_max == stored_max
        if kvstore.set(key, [new_min, new_max], :ttl => ttl, :cas => cas)
          return [:grew, (stored_min - new_min) + (new_max - stored_max)]
        else
          # collision
          retry_update(key, min_max, ttl, errors)
          return nil
        end
      end
    end

    private

    def min(a, b)
      a.nil? ? b : b.nil? ? nil : (a < b) ? a : b
    end

    def max(a, b)
      a.nil? ? b : b.nil? ? nil : (a < b) ? b : a
    end
  end
end