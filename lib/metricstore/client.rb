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
      @inserter = Inserter.new(updater_options)
      @incrementer = Incrementer.new(updater_options)
      @range_updater = RangeUpdater.new(updater_options)
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
      date = (args[:when] || Time.now).utc
      year, month, day, hour, minute = ymdhm(date)
      what = args[:what] || raise(ArgumentError, ":what is required")
      where = args[:where] || {}
      hour = [year, month, day, hour]
      minute = [year, month, day, hour, minute]
      where.to_a.all_combinations do |dimensions|
        key = counter_key(hour, what, dimensions)
        incrementer.increment(key, 1, ttl_of_hours)
        key = counter_key(minute, what, dimensions)
        incrementer.increment(key, 1, ttl_of_minutes)
      end
      where.size.times do |i|
        where2 = where.to_a
        list, value = where2.delete_at(i)
        where2.all_combinations do |dimensions|
          key = list_key(hour, what, list, dimensions)
          inserter.insert(key, value, ttl_of_hours)
          key = list_key(minute, what, list, dimensions)
          inserter.insert(key, value, ttl_of_minutes)
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
      #TODO
    end

    def count(args={})
    end

    def list(args={})
    end

    def sum(args={})
    end

    def average(args={})
    end

    def maximum(args={})
    end

    def minimum(args={})
    end

    def stddev(args={})
    end

    def count_of_groups(args={})
    end

    def sum_of_ranges(args={})
    end

    def average_range(args={})
    end

    def maximum_range(args={})
    end

    def minimum_range(args={})
    end

    def stddev_of_ranges(args={})
    end

    def estimated_list_size(args={})
    end

    private

    attr_reader :inserter
    attr_reader :incrementer
    attr_reader :range_updater

    attr_reader :sleep_interval
    attr_reader :max_healthy_errors
    attr_reader :max_unhandled_errors
    attr_reader :max_retry_delay_in_seconds

    def ymdhm(date)
      [date.year, date.month, date.day, date.hour, date.min]
    end

    def counter_key(time_block, metric_name, dimensions={})
      #TODO
    end

    def list_key(time_block, metric_name, list_name, dimensions={})
      #TODO
    end
  end
end