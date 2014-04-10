# -*- encoding: utf-8 -*-

Gem::Specification.new("metricsd-ruby", "0.0.0") do |s|
  s.authors = ["Premise"]
  s.email = "github@premise.com"

  s.summary = "A ruby metricsd client"
  s.description = "A ruby metricsd client (https://github.com/premisedata/metricsd)"

  s.homepage = "https://github.com/premisedata/metricsd-ruby"
  s.licenses = %w[MIT]

  s.extra_rdoc_files = %w[LICENSE.txt README.rdoc]

  if $0 =~ /gem/ # If running under rubygems (building), otherwise, just leave
    s.files         = `git ls-files`.split($\)
    s.test_files    = s.files.grep(%r{^(test|spec|features)/})
  end

  s.add_development_dependency "minitest", ">= 3.2.0"
  s.add_development_dependency "yard"
  s.add_development_dependency "simplecov", ">= 0.6.4"
  s.add_development_dependency "rake"
end

