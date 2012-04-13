module Metricstore
  module BaseClient

    def initialize
      @ttl_of_hours = 31_556_926 # 1 year
      @ttl_of_minutes = 86_400 # 24 hours
      @ttl_of_group_members = 7200 # 2 hours
      @list_threshold = 1000
    end

    attr_accessor :ttl_of_hours
    attr_accessor :ttl_of_minutes
    attr_accessor :ttl_of_group_members
    attr_accessor :list_threshold

    def counter(args={})
      #TODO
    end

    def measure(args={})
      #TODO
    end
  end
end