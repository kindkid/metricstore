require 'set'

module Metricstore
  class Inserter < Updater

    def insert(key, values, ttl=nil)
      update(key, values, ttl)
    end

    def list_threshold=(threshold)
      @list_threshold = threshold
    end

    def list_threshold
      @list_threshold ||= 100
    end

    protected

    def prepare_data(values)
      Set.new(Array(values))
    end

    def consolidate_data(values1, values2)
      return 'overflow' if values1 == 'overflow' || values2 == 'overflow'
      consolidated = values1 + values2
      return 'overflow' if consolidated.size > list_threshold
      consolidated
    end

    # Returns a list of the values that were newly inserted, or else nil
    # if there was contention, and we have to retry.
    def handle_update(key, values, ttl, errors)
      return [] if values.nil? || values.empty?
      #TODO: there's room here for a local cache optimization
      list, cas = kvstore.fetch(key, :ttl => ttl)
      if list.nil?
        if values == 'overflow' || values.size > list_threshold
          if kvstore.add(key, 'overflow', :ttl => ttl)
            return []
          else
            # collision
            retry_update(key, 'overflow', ttl, errors)
            return nil
          end
        elsif kvstore.add(key, values.to_a, :ttl => ttl)
          return values
        else
          # collision
          retry_update(key, values, ttl, errors)
          return nil
        end
      elsif list == 'overflow'
        return []
      else
        list = Set.new(list)
        values = values.reject{ |v| list.include?(v) }
        return [] if values.empty?
        new_list = values + list.to_a
        if new_list.size > list_threshold
          if kvstore.set(key, 'overflow', :cas => cas, :ttl => ttl)
            return []
          else
            # collision
            retry_update(key, 'overflow', ttl, errors)
            return nil
          end
        elsif kvstore.set(key, new_list, :cas => cas, :ttl => ttl)
          return values
        else
          # collision
          retry_update(key, values, ttl, errors)
          return nil
        end
      end
    end
  end
end 