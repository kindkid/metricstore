module Metricstore
  module AllCombinations
    def all_combinations
      if block_given?
        a = self.to_a
        0.upto(size) do |n|
          a.combination(n) do |c|
            yield c
          end
        end
      else
        Enumerator.new do |yielder|
          a = self.to_a
          0.upto(size) do |n|
            a.combination(n) do |c|
              yielder << c
            end
          end
        end
      end
    end
  end
end

class Array
  include Metricstore::AllCombinations
end
