require 'eventmachine'

module Metricstore

  # Abstract class. Not thread-safe.
  #
  # Sub-classes must implement (protected) methods:
  #
  #   prepare_data(data)
  #
  #   consolidate_data(data1, data2)
  #
  #   handle_update(key, data, ttl, errors)
  #    -> must return a truthy value if and only if the update occurred.
  #
  class Updater

    # opts:
    #   :sleep_interval - sleep cycle length in seconds (default: 0.1).
    #   :kvstore  - the underlying key-value store.
    #   :max_retry_delay_in_seconds - maximum length of time to wait after an error.
    #   :max_unhandled_errors - maximum number of retries before handling errors. Set this >= max_healthy_errors.
    #   :max_healthy_errors - maximum number of retries before healhty? returns false. Set this <= max_unhandled_errors.
    def initialize(opts={})
      @sleep_interval = (opts[:sleep_interval] || 0.1).to_f
      @kvstore = required(opts, :kvstore)
      @max_retry_delay = required(opts, :max_retry_delay_in_seconds).to_f
      @max_unhandled_errors = required(opts, :max_unhandled_errors).to_i
      @max_healthy_errors = required(opts, :max_healthy_errors).to_i

      @timer = nil
      @running = false
      @healthy = nil
      @pending_updates = {}
    end

    def start!
      return if @running
      @running = true
      EM.next_tick { process! }
    end

    # Be sure to call this after tests, when you want to let go of the object.
    def stop!
      @running = false
      if timer = @timer
        EM.cancel_timer(timer)
        @timer = nil
      end
    end

    def healthy?
      @healthy != false
    end

    # Approximate length of the queue
    def backlog
      @pending_updates.size
    end

    attr_accessor :handle_update_result

    protected

    attr_reader :kvstore

    def required(opts, parameter_name)
      opts[parameter_name] || raise("Missing parameter: #{parameter_name}")
    end

    def retry_update(key, data, ttl=nil, errors=[])
      update(key, data, ttl, errors)
    end

    def update(key, data, ttl=nil, errors=[])
      schedule(errors) do
        pend_update(key, data, ttl, errors)
      end
    end

    def consolidate_data(data1, data2)
      raise NotImplementedError
    end

    def prepare_data(data)
      raise NotImplementedError
    end

    # Sub-classes may want to over-ride this. 
    def handle_error(e)
      if defined?(Airbrake) && Airbrake.configuration.environment_name && Airbrake.configuration.public?
        Airbrake.notify(e)
      elsif defined?(Exceptional) && Exceptional::Config.should_send_to_api?
        Exceptional.handle(e)
      else
        puts e.inspect
        raise
      end
    end

    private

    attr_reader :sleep_interval
    attr_reader :max_healthy_errors
    attr_reader :max_retry_delay
    attr_reader :max_unhandled_errors

    def schedule(errors=[], &callback)
      if errors.size == 0
        EM.next_tick { callback.call }
      else
        EM.add_timer(retry_delay_for(errors)) { callback.call }
      end
    end

    def retry_delay_for(errors)
      [2 ** (errors.size - 4), max_retry_delay / 2.0].min * (1 + rand)
    end

    # This must only be called by the EM reactor thread
    def pend_update(key, data, ttl, errors)
      pending = @pending_updates[key]
      if pending.nil?
        @pending_updates[key] = {:data => prepare_data(data), :ttl => ttl, :errors => errors}
      else
        pending[:data]    = consolidate_data(pending[:data], prepare_data(data))
        pending[:ttl]     = max_ttl(ttl, pending[:ttl])
        pending[:errors] += errors
      end
    rescue => e
      handle_error(e)
    end

    def max_ttl(a, b)
      return 0 if a.nil? || b.nil? || a == 0 || b == 0
      [a,b].max
    end

    def reschedule_process!(sleep_first)
      if @running
        if timer = @timer
          EM.cancel_timer(timer)
        end
        if sleep_first
          @timer = EM.add_timer(sleep_interval) { process! }
        else
          EM.next_tick { process! }
        end
      end
    end

    def process!
      @timer = nil
      processed = 0
      until @pending_updates.empty? || (processed+=1) > 10
        key, update = @pending_updates.shift
        process_update(key, update[:data], update[:ttl], update[:errors] || [])
      end
      reschedule_process!(@pending_updates.empty?)
    rescue => e
      handle_error(e)
    end

    def process_update(key, data, ttl, errors)
      result = handle_update(key, data, ttl, errors)
      unless result.nil?
        @healthy = true
        handle_update_result.call(key, result, ttl) if handle_update_result
      end
    rescue => e
      # Uh oh. We stick the update back in the queue before handling the error.
      begin
        errors << e
        @healthy = false if errors.size > max_healthy_errors
        if errors.size <= max_unhandled_errors
          update(key, data, ttl, errors)
        else
          update(key, data, ttl, [])
          handle_each_error(errors)
        end
      rescue => e2
        handle_error(e2) # bugs on bugs on bugs if you get here!
      end
    end

    def handle_each_error(errors)
      errors.uniq{|error| [error.message, error.backtrace]}.each do |error|
        handle_error(error)
      end
    end
  end
end
