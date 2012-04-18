module Metricstore
  class RangeUpdater < Updater

    def update_range(key, value, ttl=nil)
      raise(ArgumentError, "value must be numeric") unless value.is_a?(Numeric)
      update(key, [value, value], ttl)
    end

    protected

    def prepare_data(range)
      range
    end

    def consolidate(range1, range2)
      [min(range1.first, range2.first), max(range1.last, range2.last)]
    end

    # Returns the difference in size that the range grew by, or else nil
    # if there was contention, and we have to retry.
    def handle_update(key, range, ttl, errors)
      #TODO: there's room here for a local cache optimization
      stored_range, cas = kvstore.fetch(key, :ttl => ttl)
      if stored_range.nil?
        if kvstore.add(key, range, :ttl => ttl)
          return (range.last - range.first)
        else
          # collision
          retry_update(key, range, ttl, errors)
          return nil
        end
      else
        stored_min, stored_max = stored_range
        new_min = min(stored_min, range.first)
        new_max = max(stored_max, range.last)
        return 0 if new_min == stored_min && new_max == stored_max
        if kvstore.set(key, [new_min, new_max], :ttl => ttl, :cas => cas)
          return (stored_min - new_min) + (new_max - stored_max)
        else
          # collision
          retry_update(key, range, ttl, errors)
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