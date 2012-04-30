module Metricstore
  module HyperLogLog

    HASH_BIT_SIZE = 512
    
    ALPHA = {}
    ALPHA[16] = 0.673 # m = 2**4
    ALPHA[32] = 0.697 # m = 2**5
    ALPHA[64] = 0.709 # m = 2**6
    (7 .. 16).each do |b|
      m = 2 ** b
      ALPHA[m] = 0.7213/(1 + 1.1079/m)
    end

    class Builder
      require 'digest/sha2'

      attr_reader :bucket_count
      
      # bucket_updater must have a method named "call" which takes two arguments
      #   the bucket index, and an integer value (of which it will track the max
      #   value per bucket).
      def initialize(error_rate, bucket_updater)
        @error_rate = error_rate
        unless @error_rate > 0 && @error_rate < 1
          raise(ArgumentError, "error_rate must be between 0 and 1")
        end
        @bits = HyperLogLog.bits_needed(error_rate)
        @bucket_count = 1 << @bits
        @alpha = ALPHA[@bucket_count]
        @bucket_updater = bucket_updater
      end

      def add(item)
        hashed = hash_of(item)
        bucket_index = hashed[0,@bits].to_i(2)
        raise("BUG!") if bucket_index >= @bucket_count
        next_on_bit_index = hashed[@bits .. -1].index('1')
        if next_on_bit_index.nil?
          next_on_bit_index= HASH_BIT_SIZE - @bits
        else
          next_on_bit_index += 1
        end
        @bucket_updater.call(bucket_index, next_on_bit_index)
      end

      private

      def hash_of(item)
        sha = Digest::SHA2.new(HASH_BIT_SIZE)
        sha << item.to_s
        "%#{HASH_BIT_SIZE}b" % sha.to_s.to_i(16)
      end
    end

    def self.bits_needed(error_rate)
      Math.log((1.04 / error_rate) ** 2, 2).round
    end

    def self.estimate_cardinality(buckets)
      values = buckets.to_a
      m = values.size
      raise("BUG!") unless m > 0
      alpha = ALPHA[m]
      raw = alpha * (m ** 2) / values.map{|x| 2 ** -(x || 0)}.inject(:+)
      puts "raw: #{raw}, m: #{m}"
      if raw <= 2.5 * m
        # correct for being below ideal range
        zero_registers = values.count(nil)
        if zero_registers == 0
          raw
        else
          m * Math.log(m.to_f / zero_registers)
        end
      elsif raw <= (2 ** HASH_BIT_SIZE) / 30.0
        # ideal range
        raw
      else
        # correct for being beyond ideal range
        (-2 ** HASH_BIT_SIZE) * Math.log(1 - raw.to_f/(2**HASH_BIT_SIZE), 2)
      end
    end
  end
end
