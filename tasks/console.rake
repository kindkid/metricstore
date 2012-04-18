desc "Open development console"
task :console do
  puts "Loading development console..."
  system "irb -r #{File.join('.', 'lib', 'metricstore')}"
end