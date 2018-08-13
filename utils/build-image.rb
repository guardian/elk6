#!/usr/bin/env ruby

require 'yaml'
require 'json'

#START MAIN
my_dir = File.expand_path(File.dirname(__FILE__))
puts "Running from #{my_dir}"

ami_name ="packerbuild_#{ARGV[0]}_#{DateTime.now.strftime('%Y%m%d%H%M%S')}"

common_data=nil
if ARGV[1]=="docker"
  open File.expand_path(my_dir + "/../packer/packer-common-docker.yaml"),"r" do |f|
    common_data = YAML.load(f.read())
  end #open
else
  open File.expand_path(my_dir + "/../packer/packer-common.yaml"),"r" do |f|
    common_data = YAML.load(f.read())
  end #open
  common_data['builders'][0]['ami_name'] = ami_name
end



specific_content = "/../packer/packer-#{ARGV[0]}.yaml"
specific_data=nil
open File.expand_path(my_dir + specific_content),"r" do |f|
  specific_data = YAML.load(f.read())
end #open

combined_data = common_data.merge(specific_data) do |key,oldval,newval|
  oldval + newval
end

packer_json_name = File.expand_path(my_dir + "/../packer/tempbuild.json")
open packer_json_name,"w" do |f|
  f.write(JSON.generate(combined_data))
end

Dir.chdir(File.expand_path(my_dir + "/../packer"))
system("packer build #{packer_json_name}")
