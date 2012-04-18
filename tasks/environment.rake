desc "Load the environment"
task :environment do
  require File.expand_path('../../lib/metricstore', __FILE__)
end