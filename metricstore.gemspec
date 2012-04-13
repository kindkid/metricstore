# -*- encoding: utf-8 -*-
require File.expand_path('../lib/metricstore/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Chris Johnson"]
  gem.email         = ["chris@kindkid.com"]
  gem.description   = "Metrics in a key-value store."
  gem.summary       = "Metrics in a key-value store. Accepts, summarizes, and stores continuous metrics updates to a key-value store. Answers queries in constant time."
  gem.homepage      = "https://github.com/kindkid/metricstore"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "metricstore"
  gem.require_paths = ["lib"]
  gem.version       = Metricstore::VERSION

  gem.add_dependency "couchbase", "~> 1.1.1"

  gem.add_development_dependency "rspec", "~> 2.9.0"
  gem.add_development_dependency "simplecov", "~> 0.6.1"
  gem.add_development_dependency("rb-fsevent", "~> 0.9.1") if RUBY_PLATFORM =~ /darwin/i
  gem.add_development_dependency "guard", "~> 1.0.1"
  gem.add_development_dependency "guard-bundler", "~> 0.1.3"
  gem.add_development_dependency "guard-rspec", "~> 0.7.0"
end
