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
      @ttl_of_minutes = 86_400 # 24 hours
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

    def run
      #TODO: start EM reactor if it's not already
    end

    def open
      #TODO: start EM reactor if it's not already. remember if we need to shut it down.
      #check if a block was provided. if so, yield, and close before returning.
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
    attr_accessor :ttl_of_minutes
    attr_accessor :ttl_of_group_members
    attr_accessor :list_threshold

    # A write method.
    # :what => a String. Required.
    # :when => a Time. Defaults to "now".
    # :where => a Hash<String, String> (dimension_name => value).
    # Time complexity of this method grows factorially with the size of the :where hash.
    def counter(args={})
      assert_open!
      date = (args[:when] || Time.now).utc
      ymdhm = ymdhm(date)
      ymdh = ymdhm[0,4]
      metric = required(args, :what)
      where = args[:where] || {}
      where.to_a.all_combinations do |dimensions|
        key = counter_key(ymdh, metric, dimensions)
        count_incrementer.increment(key, 1, ttl_of_hours)
        key = counter_key(ymdhm, metric, dimensions)
        count_incrementer.increment(key, 1, ttl_of_minutes)
      end
      where.size.times do |i|
        where2 = where.to_a
        list, dimension_value = where2.delete_at(i)
        where2.all_combinations do |dimensions|
          key = list_key(ymdh, metric, list, dimensions)
          inserter.insert(key, dimension_value, ttl_of_hours)
          key = list_key(ymdhm, metric, list, dimensions)
          inserter.insert(key, dimension_value, ttl_of_minutes)
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
      date = (args[:when] || Time.now).utc
      ymdhm = ymdhm(date)
      ymdh = ymdhm[0,4]
      metric = required(args, :what)
      where = args[:where] || {}
      where.to_a.all_combinations do |dimensions|
        key = counter_key(ymdh, metric, dimensions)
        count_incrementer.increment(key, 1, ttl_of_hours)
        key = counter_key(ymdhm, metric, dimensions)
        count_incrementer.increment(key, 1, ttl_of_minutes)
        key = sum_key(ymdh, metric, dimensions)
        incrementer.increment(key, value, ttl_of_hours)
        key = sum_key(ymdhm, metric, dimensions)
        incrementer.increment(key, value, ttl_of_minutes)
        key = range_key(ymdh, metric, dimensions)
        range_updater.update_range(key, value, ttl_of_hours)
        key = range_key(ymdhm, metric, dimensions)
        range_updater.update_range(key, value, ttl_of_minutes)
        key = sumsqr_key(ymdh, metric, dimensions)
        incrementer.increment(key, value*value, ttl_of_hours)
        key = sumsqr_key(ymdhm, metric, dimensions)
        incrementer.increment(key, value*value, ttl_of_minutes)
      end
      where.size.times do |i|
        where2 = where.to_a
        list, dimension_value = where2.delete_at(i)
        where2.all_combinations do |dimensions|
          key = list_key(ymdh, metric, list, dimensions)
          inserter.insert(key, dimension_value, ttl_of_hours)
          key = list_key(ymdhm, metric, list, dimensions)
          inserter.insert(key, dimension_value, ttl_of_minutes)
        end
      end
    end

    def count(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = counter_key(parse_time_block(time_block), metric_name, dimensions)
      result, cas = kvstore.fetch(key)
      result || 0
    end

    def list(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      list_name = required(args, :list)
      dimensions = args[:where] || {}
      key = list_key(parse_time_block(time_block), metric_name, list_name, dimensions)
      result, cas = kvstore.fetch(key)
      result || []
    end

    def sum(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = sum_key(parse_time_block(time_block), metric_name, dimensions)
      result, cas = kvstore.fetch(key)
      result || 0
    end

    def average(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      parsed_time_block = parse_time_block(time_block)
      count, cas = kvstore.fetch(counter_key(parsed_time_block, metric_name, dimensions))
      sum, cas = kvstore.fetch(sum_key(parsed_time_block, metric_name, dimensions))
      return nil if count.nil? || sum.nil? || count == 0
      sum.to_f / count
    end

    def maximum(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = range_key(parse_time_block(time_block), metric_name, dimensions)
      range, cas = kvstore.fetch(key)
      range.nil? ? nil : range[1]
    end

    def minimum(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = range_key(parse_time_block(time_block), metric_name, dimensions)
      range, cas = kvstore.fetch(key)
      range.nil? ? nil : range[0]
    end

    def stddev(args={})
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      parsed_time_block = parse_time_block(time_block)
      count, cas = kvstore.fetch(counter_key(parsed_time_block, metric_name, dimensions))
      sum, cas = kvstore.fetch(sum_key(parsed_time_block, metric_name, dimensions))
      sumsqr, cas = kvstore.fetch(sumsqr_key(parsed_time_block, metric_name, dimensions))
      return nil if count.nil? || sum.nil? || sumsqr.nil? || count == 0
      Math.sqrt(count * sumsqr - sum*sum) / count
    end

    def count_of_groups(args={})
      group = required(args, :group)
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = group_counter_key(parse_time_block(time_block), metric_name, group, dimensions)
      result, cas = kvstore.fetch(key)
      result || 0
    end

    def sum_of_ranges(args={})
      group = required(args, :group)
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = range_sum_key(parse_time_block(time_block), metric_name, group, dimensions)
      result, cas = kvstore.fetch(key)
      result || 0
    end

    def average_range(args={})
      group = required(args, :group)
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      parsed_time_block = parse_time_block(time_block)
      count, cas = kvstore.fetch(group_counter_key(parsed_time_block, metric_name, group, dimensions))
      sum, cas = kvstore.fetch(range_sum_key(parsed_time_block, metric_name, group, dimensions))
      return nil if count.nil? || sum.nil? || count == 0
      sum.to_f / count
    end

    def maximum_range(args={})
      group = required(args, :group)
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = group_range_key(parse_time_block(time_block), metric_name, group, dimensions)
      range, cas = kvstore.fetch(key)
      range.nil? ? nil : range[1]
    end

    def minimum_range(args={})
      group = required(args, :group)
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      key = group_range_key(parse_time_block(time_block), metric_name, group, dimensions)
      range, cas = kvstore.fetch(key)
      range.nil? ? nil : range[0]
    end

    def stddev_of_ranges(args={})
      group = required(args, :group)
      time_block = required(args, :when)
      metric_name = required(args, :what)
      dimensions = args[:where] || {}
      parsed_time_block = parse_time_block(time_block)
      count, cas = kvstore.fetch(group_counter_key(parsed_time_block, metric_name, group, dimensions))
      sum, cas = kvstore.fetch(range_sum_key(parsed_time_block, metric_name, group, dimensions))
      sumsqr, cas = kvstore.fetch(range_sumsqr_key(parsed_time_block, metric_name, group, dimensions))
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

    def ymdhm(date)
      [date.year, date.month, date.day, date.hour, date.min]
    end

    def parse_time_block(time_block)
      time_block = time_block.split(/[ _\-\/\.\:]/) if time_block.is_a?(String)
      time_block.map(&:to_i)
    end

    def stringify_time_block(time_block)
      time_block.map{|t| '%02d' % t}.join('-')
    end

    def stringify_dimensions(dimensions)
      dimensions.sort.map{|k,v| CGI.escape(k.to_s) << '=' << CGI.escape(v.to_s)}.join('&')
    end

    def counter_key(time_block, metric_name, dimensions={})
      "count:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def list_key(time_block, metric_name, list_name, dimensions={})
      "list:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}/#{CGI.escape(list_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def sum_key(time_block, metric_name, dimensions={})
      "sum:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def sumsqr_key(time_block, metric_name, dimensions={})
      "sumsqr:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def range_key(time_block, metric_name, dimensions={})
      "range:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def group_counter_key(time_block, metric_name, group_name, dimensions={})
      "rangecount:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}/#{CGI.escape(group_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def group_range_key(time_block, metric_name, group_name, dimensions={})
      "rangerange:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}/#{CGI.escape(group_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def range_sum_key(time_block, metric_name, group_name, dimensions={})
      "rangesum:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}/#{CGI.escape(group_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end
    def range_sumsqr_key(time_block, metric_name, group_name, dimensions={})
      "rangesumsqr:/#{stringify_time_block(time_block)}/#{CGI.escape(metric_name.to_s)}/#{CGI.escape(group_name.to_s)}?#{stringify_dimensions(dimensions)}"
    end

    def required(args, argument_name)
      args[argument_name] || raise(ArgumentError, "missing argument: #{argument_name}")
    end

    def assert_open!
      raise "Client has not been opened" unless @open
    end
  end
end