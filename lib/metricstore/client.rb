require 'cgi'

module Metricstore
  class Client

    CARDINALITY_ESTIMATOR_ERROR_RATE = 0.05

    #   :kvstore  - the underlying key-value store.
    #   :sleep_interval - sleep cycle length in seconds (default: 0.1).
    #   :max_retry_delay_in_seconds - maximum time to wait after an error.
    #   :max_unhandled_errors - maximum retries before handling errors.
    #                           Set this >= max_healthy_errors.
    #   :max_healthy_errors - maximum retries before healthy? returns false.
    #                         Set this <= max_unhandled_errors.
    def initialize(opts={})
      @ttl_of_hours = 31_556_926 # 1 year

      @kvstore = required(opts, :kvstore)
      @sleep_interval = required(opts, :sleep_interval)
      @max_healthy_errors = required(opts, :max_healthy_errors)
      @max_unhandled_errors = required(opts, :max_unhandled_errors)
      @max_retry_delay_in_seconds = required(opts, :max_retry_delay_in_seconds)
      @max_ttl_of_dimension = {}

      updater_options = {
        :kvstore => @kvstore,
        :sleep_interval => @sleep_interval,
        :max_healthy_errors => @max_healthy_errors,
        :max_unhandled_errors => @max_unhandled_errors,
        :max_retry_delay_in_seconds => @max_retry_delay_in_seconds
      }
      @open = false
      @inserter = Inserter.new(updater_options)
      bucket_count = 1 << HyperLogLog.bits_needed(CARDINALITY_ESTIMATOR_ERROR_RATE)
      @inserter.list_threshold = (2.5 * bucket_count).ceil
      @incrementer = Incrementer.new(updater_options)
      @range_updater = RangeUpdater.new(updater_options)
      @count_incrementer = CountIncrementer.new(updater_options)

      range_updater.handle_update_result = Proc.new do |key, result, ttl|
        if key.start_with?("range:") && !result.nil?
          new_or_grew, amount = result
          if new_or_grew == :new || new_or_grew == :grew
            _, time_block, metric_name, dimensions = key.split(/[\/\?]/)
            unless dimensions.nil?
              dimensions = dimensions.split('&')
              dimensions.size.times do |i|
                dimensions2 = dimensions.clone
                group, dimension_value = dimensions2.delete_at(i).split('=')
                key_suffix = "#{time_block}/#{metric_name}/#{group}?#{dimensions2.join('&')}"
                incrementer.increment("rangesum:/#{key_suffix}", amount, ttl)
                incrementer.increment("rangesumsqr:/#{key_suffix}", amount * amount, ttl)
                range_updater.update_range("rangerange:/#{key_suffix}", amount, ttl)
                if new_or_grew == :new
                  count_incrementer.increment("rangecount:/#{key_suffix}", 1, ttl)
                end
              end
            end
          end
        end
      end
    end

    def open
      inserter.start!
      incrementer.start!
      range_updater.start!
      count_incrementer.start!
      @open = true
    end

    def close
      @open = false
      inserter.stop!
      incrementer.stop!
      range_updater.stop!
      count_incrementer.stop!
    end

    def backlog
      inserter.backlog + incrementer.backlog + range_updater.backlog + count_incrementer.backlog
    end

    attr_accessor :ttl_of_hours
    attr_accessor :max_ttl_of_dimension

    def list_threshold
      inserter.list_threshold
    end

    def list_threshold=(threshold)
      inserter.list_threshold = threshold
    end

    # A write method.
    # :what => a String. Required.
    # :when => a Time. Defaults to "now".
    # :where => a Hash<String, String> (dimension_name => value).
    # Time complexity of this method grows factorially with the size of the :where hash.
    def counter(args={})
      assert_open!
      hour = date_as_hour((args[:when] || Time.now).utc)
      metric = escape(required(args, :what).to_s)
      where = (args[:where] || {}).map{|k,v| [k, v, escape(k) << '=' << escape(v), max_ttl_of_dimension[k]] }
      where.all_combinations do |dimensions|
        key = counter_key(hour, metric, dimensions.sort.map{|k,v,s,ttl| s}.join('&'))
        ttl = (dimensions.map{|k,v,s,ttl| ttl} << ttl_of_hours).compact.min
        count_incrementer.increment(key, 1, ttl)
      end
      where.size.times do |i|
        where2 = where.clone
        list, dimension_value, _ = where2.delete_at(i)
        list = escape(list)
        key_middle = "#{hour}/#{metric}/#{list}?"
        where2.all_combinations do |dimensions|
          key_suffix = "#{key_middle}#{dimensions.sort.map{|k,v,s,ttl| s}.join('&')}"
          ttl = (dimensions.map{|k,v,s,ttl| ttl} << ttl_of_hours).compact.min
          inserter.insert("list:/#{key_suffix}", dimension_value, ttl)
          estimator = HyperLogLog::Builder.new(CARDINALITY_ESTIMATOR_ERROR_RATE, Proc.new do |idx, val|
            range_updater.update_range("hyperloglog:#{idx.to_i}:/#{key_suffix}", val, ttl)
          end)
          estimator.add(dimension_value)
        end
      end
    end

    # A write method.
    # :value => an integer. Required.
    # :what => a String. Required.
    # :when => a Time. Defaults to "now".
    # :where => a Hash<String, String> (dimension_name => value).
    # Time complexity of this method grows factorially with the size of the :where hash.
    def measure(args={})
      assert_open!
      value = required(args, :value).to_i
      hour = date_as_hour((args[:when] || Time.now).utc)
      metric = escape(required(args, :what).to_s)
      where = (args[:where] || {}).map{|k,v| [k, v, escape(k) << '=' << escape(v), max_ttl_of_dimension[k]] }
      where.all_combinations do |dimensions|
        dimensions_string = dimensions.sort.map{|k,v,s,ttl| s}.join('&')
        ttl = (dimensions.map{|k,v,s,ttl| ttl} << ttl_of_hours).compact.min
        suffix = build_key('', hour, metric, dimensions_string)
        count_incrementer.increment("count#{suffix}", 1, ttl)
        incrementer.increment("sum#{suffix}", value, ttl)
        range_updater.update_range("range#{suffix}", value, ttl)
        incrementer.increment("sumsqr#{suffix}", value*value, ttl)
      end
      where.size.times do |i|
        where2 = where.clone
        list, dimension_value, _ = where2.delete_at(i)
        list = escape(list)
        key_middle = "#{hour}/#{metric}/#{list}?"
        where2.all_combinations do |dimensions|
          key_suffix = "#{key_middle}#{dimensions.sort.map{|k,v,s,ttl| s}.join('&')}"
          ttl = (dimensions.map{|k,v,s,ttl| ttl} << ttl_of_hours).compact.min
          inserter.insert("list:/#{key_suffix}", dimension_value, ttl)
          estimator = HyperLogLog::Builder.new(CARDINALITY_ESTIMATOR_ERROR_RATE, Proc.new do |idx, val|
            range_updater.update_range("hyperloglog:#{idx.to_i}:/#{key_suffix}", val, ttl)
          end)
          estimator.add(dimension_value)
        end
      end
    end

    def count(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(counter_key(time_block, metric_name, dimensions))
      result || 0
    end

    def list(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      list_name = escape(required(args, :list).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(list_key(time_block, metric_name, list_name, dimensions))
      if result == 'overflow'
        error_message = "Too many #{args[:list]} for #{time_block}, #{args[:what]}"
        error_message << ", where #{args[:where].inspect}" unless dimensions.empty?
        raise(Metricstore::DataLossError, error_message)
      else
        result || []
      end
    end

    def sum(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(sum_key(time_block, metric_name, dimensions))
      result || 0
    end

    def average(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(counter_key(time_block, metric_name, dimensions))
      sum, cas = kvstore.fetch(sum_key(time_block, metric_name, dimensions))
      return nil if count.nil? || sum.nil? || count == 0
      sum.to_f / count
    end

    def maximum(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(range_key(time_block, metric_name, dimensions))
      range.nil? ? nil : range[1]
    end

    def minimum(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(range_key(time_block, metric_name, dimensions))
      range.nil? ? nil : range[0]
    end

    def stddev(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(counter_key(time_block, metric_name, dimensions))
      sum, cas = kvstore.fetch(sum_key(time_block, metric_name, dimensions))
      sumsqr, cas = kvstore.fetch(sumsqr_key(time_block, metric_name, dimensions))
      return nil if count.nil? || sum.nil? || sumsqr.nil? || count == 0
      Math.sqrt(count * sumsqr - sum*sum) / count
    end

    def count_of_groups(args={})
      group = escape(required(args, :group))
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(group_counter_key(time_block, metric_name, group, dimensions))
      result || 0
    end

    def sum_of_ranges(args={})
      group = escape(required(args, :group))
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(range_sum_key(time_block, metric_name, group, dimensions))
      result || 0
    end

    def average_range(args={})
      group = escape(required(args, :group))
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(group_counter_key(time_block, metric_name, group, dimensions))
      sum, cas = kvstore.fetch(range_sum_key(time_block, metric_name, group, dimensions))
      return nil if count.nil? || sum.nil? || count == 0
      sum.to_f / count
    end

    def maximum_range(args={})
      group = escape(required(args, :group))
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(group_range_key(time_block, metric_name, group, dimensions))
      range.nil? ? nil : range[1]
    end

    def minimum_range(args={})
      group = escape(required(args, :group))
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(group_range_key(time_block, metric_name, group, dimensions))
      range.nil? ? nil : range[0]
    end

    def stddev_of_ranges(args={})
      group = escape(required(args, :group))
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(group_counter_key(time_block, metric_name, group, dimensions))
      sum, cas = kvstore.fetch(range_sum_key(time_block, metric_name, group, dimensions))
      sumsqr, cas = kvstore.fetch(range_sumsqr_key(time_block, metric_name, group, dimensions))
      return nil if count.nil? || sum.nil? || sumsqr.nil? || count == 0
      Math.sqrt(count * sumsqr - sum*sum) / count
    end

    def estimated_list_size(args={})
      time_block = required(args, :hour)
      metric_name = escape(required(args, :what).to_s)
      list_name = escape(required(args, :list).to_s)
      dimensions = (args[:where] || {}).sort.map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      list, cas = kvstore.fetch(list_key(time_block, metric_name, list_name, dimensions))
      if list == 'overflow'
        bucket_count = 1 << HyperLogLog.bits_needed(CARDINALITY_ESTIMATOR_ERROR_RATE)
        buckets = Enumerator.new do |yielder|
          bucket_count.times do |i|
            key = hyperloglog_key(i, time_block, metric_name, list_name, dimensions)
            range, cas = kvstore.fetch(key)
            yielder << (range.nil? ? nil : range[1])
          end
        end
        HyperLogLog.estimate_cardinality(buckets)
      else
        list.size
      end
    end

    private

    attr_reader :kvstore
    attr_reader :inserter
    attr_reader :incrementer
    attr_reader :range_updater
    attr_reader :count_incrementer

    attr_reader :sleep_interval
    attr_reader :max_healthy_errors
    attr_reader :max_unhandled_errors
    attr_reader :max_retry_delay_in_seconds

    def date_as_hour(date)
      date.strftime('%Y-%m-%d-%H')
    end

    def build_key(prefix, time_block, metric, dimensions, list_group=nil)
      key = ''
      key << prefix
      key << ':/'
      key << time_block
      key << '/'
      key << metric
      unless list_group.nil?
        key << '/'
        key << list_group
      end
      key << '?'
      key << dimensions
      key
    end

    def counter_key(time_block, metric, dimensions)
      build_key('count', time_block, metric, dimensions)
    end

    def list_key(time_block, metric, list_name, dimensions)
      build_key('list', time_block, metric, dimensions, list_name)
    rescue Exception => e
      puts e.message
      puts e.backtrace
      raise
    end

    def sum_key(time_block, metric, dimensions)
      build_key('sum', time_block, metric, dimensions)
    end

    def sumsqr_key(time_block, metric, dimensions)
      build_key('sumsqr', time_block, metric, dimensions)
    end

    def range_key(time_block, metric, dimensions)
      build_key('range', time_block, metric, dimensions)
    end

    def group_counter_key(time_block, metric, group_name, dimensions)
      build_key('rangecount', time_block, metric, dimensions, group_name)
    end

    def group_range_key(time_block, metric, group_name, dimensions)
      build_key('rangerange', time_block, metric, dimensions, group_name)
    end

    def range_sum_key(time_block, metric, group_name, dimensions)
      build_key('rangesum', time_block, metric, dimensions, group_name)
    end

    def range_sumsqr_key(time_block, metric, group_name, dimensions)
      build_key('rangesumsqr', time_block, metric, dimensions, group_name)
    end

    def hyperloglog_key(index, time_block, metric, list_name, dimensions)
      build_key("hyperloglog:#{index.to_i}", time_block, metric, dimensions, list_name)
    end

    def required(args, argument_name)
      args[argument_name] || raise(ArgumentError, "missing argument: #{argument_name}")
    end

    def assert_open!
      raise "Client has not been opened" unless @open
    end

    def escape(s)
      CGI.escape(s.to_s)
    end
  end
end