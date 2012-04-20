# A sample Guardfile
# More info at https://github.com/guard/guard#readme

guard 'bundler' do
  watch('Gemfile')
  watch(%r{.+\.gemspec})
end

guard 'rspec', :cli => '-c --format documentation -r ./spec/spec_helper.rb',
               :version => 2 do
  watch(%r{^spec/.+_spec\.rb})
  watch(%r{^lib/(.+)\.rb})           { |m| "spec/lib/#{m[1]}_spec.rb" }
  watch('spec/spec_helper.rb')       { "spec/" }
  watch('lib/metricstore.rb')        { "spec/" }
end
