require "bundler/gem_tasks"

require 'bundler/setup'
Bundler.require(:default)

Dir['tasks/*.rake'].sort.each { |task| load task }

# Add rake tasks from selected gems
gem_names = []
gem_names.each do |gem_name|
  Dir[File.join(Gem.searcher.find(gem_name).full_gem_path, '**', '*.rake')].each{|rake_file| load rake_file }
end