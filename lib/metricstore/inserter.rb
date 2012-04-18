require 'set'

module Metricstore
  class Inserter < Updater

    def insert(key, values, ttl=nil)
      update(key, values, ttl)
    end

    protected

    def prepare_data(values)
      Set.new(Array(values))
    end

    def consolidate(values1, values2)
      values1 + values2
    end

    # Returns a list of the values that were newly inserted, or else nil
    # if there was contention, and we have to retry.
    def handle_update(key, values, ttl, errors)
      return [] if values.nil? || values.empty?
      #TODO: there's room here for a local cache optimization
      list, cas = kvstore.fetch(key, :ttl => ttl)
      if list.nil?
        if kvstore.add(key, values.to_a, :ttl => ttl)
          return values
        else
          # collision
          retry_update(key, values, ttl, errors)
          return nil
        end
      else
        list = Set.new(list)
        values = values.reject{ |v| list.include?(v) }
        return [] if values.empty?
        if kvstore.set(key, values + list.to_a, :cas => cas, :ttl => ttl)
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