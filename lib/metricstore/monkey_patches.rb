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

class Hash
  def all_combinations
    if block_given?
      self.to_a.all_combinations do |c|
        yield Hash[c]
      end
    else
      Enumerator.new do |yielder|
        self.to_a.all_combinations do |c|
          yielder << Hash[c]
        end
      end
    end
  end
end