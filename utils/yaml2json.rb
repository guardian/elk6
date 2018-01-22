#!/usr/bin/env ruby

require 'yaml'
require 'json'

#START MAIN
open ARGV[0],"r" do |f|
  data = YAML.load(f.read())
  print JSON.generate(data)
end #open