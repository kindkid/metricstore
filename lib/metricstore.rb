require "metricstore/version"
require "metricstore/exceptions"
require "metricstore/monkey_patches"
require "metricstore/client"
require "metricstore/couchbase_client"
require "metricstore/mock_key_value_client"
require "metricstore/updater"
require "metricstore/count_incrementer"
require "metricstore/incrementer"
require "metricstore/inserter"
require "metricstore/range_updater"

module Metricstore
  
  def self.couchbase(*args, &callback)
    couchbase_client = CouchbaseClient.new(*args, &callback)
    Client.new(
      :kvstore => couchbase_client,
      :sleep_interval => 0.1,
      :max_healthy_errors => 2,
      :max_unhandled_errors => 8,
      :max_retry_delay_in_seconds => 60.0
    )
  end

end
