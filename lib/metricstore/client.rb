require 'cgi'

module Metricstore
  class Client

    #   :kvstore  - the underlying key-value store.
    #   :sleep_interval - sleep cycle length in seconds (default: 0.1).
    #   :max_retry_delay_in_seconds - maximum time to wait after an error.
    #   :max_unhandled_errors - maximum retries before handling errors.
    #                           Set this >= max_healthy_errors.
    #   :max_healthy_errors - maximum retries before healthy? returns false.
    #                         Set this <= max_unhandled_errors.
    def initialize(opts={})
      @ttl_of_hours = 31_556_926 # 1 year
      @ttl_of_group_members = 7200 # 2 hours
      @list_threshold = 1000

      @kvstore = required(opts, :kvstore)
      @sleep_interval = required(opts, :sleep_interval)
      @max_healthy_errors = required(opts, :max_healthy_errors)
      @max_unhandled_errors = required(opts, :max_unhandled_errors)
      @max_retry_delay_in_seconds = required(opts, :max_retry_delay_in_seconds)

      updater_options = {
        :kvstore => @kvstore,
        :sleep_interval => @sleep_interval,
        :max_healthy_errors => @max_healthy_errors,
        :max_unhandled_errors => @max_unhandled_errors,
        :max_retry_delay_in_seconds => @max_retry_delay_in_seconds
      }
      @open = false
      @inserter = Inserter.new(updater_options)
      @incrementer = Incrementer.new(updater_options)
      @range_updater = RangeUpdater.new(updater_options)
      @count_incrementer = CountIncrementer.new(updater_options)

      @range_updater.handle_update_result = Proc.new do |key, result, ttl|
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
                @incrementer.increment("rangesum:/#{key_suffix}", amount, ttl)
                @incrementer.increment("rangesumsqr:/#{key_suffix}", amount * amount, ttl)
                @range_updater.update_range("rangerange:/#{key_suffix}", amount, ttl)
                if new_or_grew == :new
                  @count_incrementer.increment("rangecount:/#{key_suffix}", 1, ttl)
                end
              end
            end
          end
        end
      end
    end

    def open
      @inserter.start!
      @incrementer.start!
      @range_updater.start!
      @count_incrementer.start!
      @open = true
    end

    def close
      @open = false
      @inserter.stop!
      @incrementer.stop!
      @range_updater.stop!
      @count_incrementer.stop!
    end

    attr_accessor :ttl_of_hours
    attr_accessor :ttl_of_group_members
    attr_accessor :list_threshold

    # A write method.
    # :what => a String. Required.
    # :when => a Time. Defaults to "now".
    # :where => a Hash<String, String> (dimension_name => value).
    # Time complexity of this method grows factorially with the size of the :where hash.
    def counter(args={})
      assert_open!
      date = stringify_date((args[:when] || Time.now).utc)
      metric = escape(required(args, :what).to_s)
      where = (args[:where] || {}).map{|k,v| [k, v, escape(k) << '=' << escape(v)] }
      where.all_combinations do |dimensions|
        key = counter_key(date, metric, dimensions.map{|k,v,s| s}.join('&'))
        count_incrementer.increment(key, 1, ttl_of_hours)
      end
      where.size.times do |i|
        where2 = where.clone
        list, dimension_value, _ = where2.delete_at(i)
        list = escape(list)
        where2.all_combinations do |dimensions|
          key = list_key(date, metric, list, dimensions.map{|k,v,s| s}.join('&'))
          inserter.insert(key, dimension_value, ttl_of_hours)
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
      date = stringify_date((args[:when] || Time.now).utc)
      metric = escape(required(args, :what).to_s)
      where = (args[:where] || {}).map{|k,v| [k, v, escape(k) << '=' << escape(v)] }
      where.all_combinations do |dimensions|
        dimensions_string = dimensions.map{|k,v,s| s}.join('&')
        suffix = build_key('', date, metric, dimensions_string)
        count_incrementer.increment("count#{suffix}", 1, ttl_of_hours)
        incrementer.increment("sum#{suffix}", value, ttl_of_hours)
        range_updater.update_range("range#{suffix}", value, ttl_of_hours)
        incrementer.increment("sumsqr#{suffix}", value*value, ttl_of_hours)
      end
      where.size.times do |i|
        where2 = where.clone
        list, dimension_value, _ = where2.delete_at(i)
        list = escape(list)
        prefix = list_key(date, metric, list, '')
        where2.all_combinations do |dimensions|
          key = "#{prefix}#{dimensions.map{|k,v,s| s}.join('&')}"
          inserter.insert(key, dimension_value, ttl_of_hours)
        end
      end
    end

    def count(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(counter_key(time_block, metric_name, dimensions))
      result || 0
    end

    def list(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      list_name = escape(required(args, :list).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(list_key(time_block, metric_name, list_name, dimensions))
      result || []
    end

    def sum(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(sum_key(time_block, metric_name, dimensions))
      result || 0
    end

    def average(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(counter_key(time_block, metric_name, dimensions))
      sum, cas = kvstore.fetch(sum_key(time_block, metric_name, dimensions))
      return nil if count.nil? || sum.nil? || count == 0
      sum.to_f / count
    end

    def maximum(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(range_key(time_block, metric_name, dimensions))
      range.nil? ? nil : range[1]
    end

    def minimum(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(range_key(time_block, metric_name, dimensions))
      range.nil? ? nil : range[0]
    end

    def stddev(args={})
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(counter_key(time_block, metric_name, dimensions))
      sum, cas = kvstore.fetch(sum_key(time_block, metric_name, dimensions))
      sumsqr, cas = kvstore.fetch(sumsqr_key(time_block, metric_name, dimensions))
      return nil if count.nil? || sum.nil? || sumsqr.nil? || count == 0
      Math.sqrt(count * sumsqr - sum*sum) / count
    end

    def count_of_groups(args={})
      group = escape(required(args, :group))
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(group_counter_key(time_block, metric_name, group, dimensions))
      result || 0
    end

    def sum_of_ranges(args={})
      group = escape(required(args, :group))
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      result, cas = kvstore.fetch(range_sum_key(time_block, metric_name, group, dimensions))
      result || 0
    end

    def average_range(args={})
      group = escape(required(args, :group))
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(group_counter_key(time_block, metric_name, group, dimensions))
      sum, cas = kvstore.fetch(range_sum_key(time_block, metric_name, group, dimensions))
      return nil if count.nil? || sum.nil? || count == 0
      sum.to_f / count
    end

    def maximum_range(args={})
      group = escape(required(args, :group))
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(group_range_key(time_block, metric_name, group, dimensions))
      range.nil? ? nil : range[1]
    end

    def minimum_range(args={})
      group = escape(required(args, :group))
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      range, cas = kvstore.fetch(group_range_key(time_block, metric_name, group, dimensions))
      range.nil? ? nil : range[0]
    end

    def stddev_of_ranges(args={})
      group = escape(required(args, :group))
      time_block = required(args, :when)
      metric_name = escape(required(args, :what).to_s)
      dimensions = (args[:where] || {}).map{|k,v| escape(k) << '=' << escape(v)}.join('&')
      count, cas = kvstore.fetch(group_counter_key(time_block, metric_name, group, dimensions))
      sum, cas = kvstore.fetch(range_sum_key(time_block, metric_name, group, dimensions))
      sumsqr, cas = kvstore.fetch(range_sumsqr_key(time_block, metric_name, group, dimensions))
      return nil if count.nil? || sum.nil? || sumsqr.nil? || count == 0
      Math.sqrt(count * sumsqr - sum*sum) / count
    end

    def estimated_list_size(args={})
      group = required(args, :list)
      count_of_groups(args.merge(:group => group))
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

    def stringify_date(date)
      date.strftime('%Y-%m-%d-%H')
    end

    def build_key(prefix, date, metric, dimensions, list_group=nil)
      key = ''
      key << prefix
      key << ':/'
      key << date
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

    def counter_key(date, metric, dimensions)
      build_key('count', date, metric, dimensions)
    end

    def list_key(date, metric, list_name, dimensions)
      build_key('list', date, metric, dimensions, list_name)
    rescue Exception => e
      puts e.message
      puts e.backtrace
      raise
    end

    def sum_key(date, metric, dimensions)
      build_key('sum', date, metric, dimensions)
    end

    def sumsqr_key(date, metric, dimensions)
      build_key('sumsqr', date, metric, dimensions)
    end

    def range_key(date, metric, dimensions)
      build_key('range', date, metric, dimensions)
    end

    def group_counter_key(date, metric, group_name, dimensions)
      build_key('rangecount', date, metric, dimensions, group_name)
    end

    def group_range_key(date, metric, group_name, dimensions)
      build_key('rangerange', date, metric, dimensions, group_name)
    end

    def range_sum_key(date, metric, group_name, dimensions)
      build_key('rangesum', date, metric, dimensions, group_name)
    end
    def range_sumsqr_key(date, metric, group_name, dimensions)
      build_key('rangesumsqr', date, metric, dimensions, group_name)
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