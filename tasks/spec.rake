require 'rspec/core/rake_task'

desc "Run all specs"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = [
    '-c',
    '--format documentation',
    '-r ./spec/spec_helper.rb',
    '--backtrace'
  ]
  t.pattern = 'spec/**/*_spec.rb'
end
