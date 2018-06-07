#!/usr/bin/env ruby

require 'trollop'
require 'awesome_print'
require 'net/http'
require 'json'
require 'logger'

$logger = Logger.new(STDOUT)
$logger.level = Logger::DEBUG

opts = Trollop::options do
  opt :cluster, "Connect to Elasticsearch at this uri", :type=>:string,:default=>"http://localhost:9200"
  opt :index, "Create this index", :type=>:string
  opt :allow_overwrite, "Allow the deletion of an existing index", :type=>:boolean, :default=>false
  opt :input, "Read json files from this directory", :type=>:string,:default=>Dir.pwd
  opt :strip_fields, "Comma separated list of fields to remove before uploading", :type=>:string,:default=>""
end

def check_cluster(address)
  $logger.debug("Connecting to #{address}...")
  uri = URI(address)
  raw_json = Net::HTTP.get(uri)
  content = JSON.parse(raw_json)
  $logger.info("Connected to #{content['cluster_name']}, ES version #{content['version']['number']}. Status is #{content['status']}")

  if content.has_key?('status') and content['status'] != 200
    ap content
    raise RuntimeError, "Server is in an error state"
  end
end #def check_cluster

def delete_index(address, indexname)
  uri = URI(File.join(address,indexname))
  $logger.debug("Connecting to #{uri}...")
  Net::HTTP.start(uri.hostname,uri.port) do |http|
    rq = Net::HTTP::Delete.new uri
    response = http.request rq
    content = JSON.parse(response.body)
    if response.code != '200'
      ap content
      raise RuntimeError, "Could not delete #{indexname}, server returned #{response.code}"
    end #if response.code
  end #Net::HTTP.start
end #delete_index

def check_index(address, indexname, allow_overwrite)
  uri = URI(File.join(address,indexname))
  $logger.debug("Connecting to #{uri}...")
  response = Net::HTTP.get_response(uri)
  content = JSON.parse(response.body)
  if response.code == '200'
    if allow_overwrite
      delete_index(address,indexname)
    else
      raise RuntimeError,  "Index #{indexname} already exists, not continuing as --allow_overwrite not specified.  If you specify this option then the existing index will be deleted."
    end
  elsif response.code != '404'
    ap content
    raise RuntimeError, "Could not access index #{indexname}, server returned a #{response.code} error"
  end

  $logger.info("Ready to write index #{indexname}")
  content
end

def locate_index_data(rootpath, indexname)
  index_config = File.join(rootpath, "indexinfo-#{indexname}.json")
  raise RuntimeError, "Could not locate file #{index_config}" unless File.exists?(index_config)

  index_data = Dir.glob(File.join(rootpath,"indexdata-#{indexname}-*.json"))
  return {
    :config=>index_config,
    :data_files=>index_data
  }
end #def locate_index_data

def create_index(address, indexname, configfile)
  configdata = File.open configfile, "r" do |f|
    JSON.parse(f.read())
  end
  #the configdata has an extra json level containing the index name that we don't need
  if configdata.values.length > 1
    raise RuntimeError, "This configdata has more than one index?"
  end

  configroot = configdata.values[0]
  #remove incompatible values
  configroot['settings']['index'].delete('creation_date')
  configroot['settings']['index'].delete('uuid')
  configroot['settings']['index']['version'].delete('created')

  uri = URI("#{address}/#{indexname}")
  $logger.debug("Connecting to #{uri}...")
  Net::HTTP.start(uri.hostname, uri.port) do |http|
    rq = Net::HTTP::Put.new uri
    rq.body = JSON.generate(configroot)
    rq['Content-Type'] = 'application/json'

    response = http.request rq
    unless response.code=='200'
      error_info = JSON.parse(response.body)
      ap error_info
      raise RuntimeError, "Could not create index, server returned #{response.code} error"
    end #unless response.code==200
  end #Net::HTTP.start
end #def create_index

def upload_data_file(address, indexname, datafile, strip_fields)
  $logger.info("Reading in #{datafile}")
  datacontent = File.open datafile, "r" do |f|
    JSON.parse(f.read())
  end

  bulkdata = datacontent['hits']['hits'].reduce("") { |acc, entry|
    entry.delete('_score')
    entry.delete('sort')
    entry_source = entry.delete('_source')
    strip_fields.each {|field_to_remove|
      #$logger.debug("Removing field #{field_to_remove}")
      entry_source.delete(field_to_remove)
    }
    acc + JSON.generate({"index"=>entry}) + "\n" + JSON.generate(entry_source) + "\n"
  }

  $logger.info("Uploading via bulk api")
  uri = URI("#{address}/_bulk")
  $logger.debug("Connecting to #{uri}...")
  Net::HTTP.start(uri.hostname, uri.port) do |http|
    rq = Net::HTTP::Post.new uri
    rq['Content-Type'] = 'application/json'
    rq.body = bulkdata+"\n"

    response = http.request rq
    reply = JSON.parse(response.body)
    #ap reply
    unless response.code=='200'
      print bulkdata
      ap reply
      raise RuntimeError, "Could not upload data, server returned #{response.code} error"
    end #unless response.code==200
  end #Net::HTTP.start
end #def upload_data_file
#START MAIN
#Establish connection
$logger.info("Starting up")
check_cluster(opts.cluster)

#Get index setup
if opts[:index].nil? then
  $logger.error("You must specify an index name on the commandline with --index")
  exit(2)
end

$logger.info("Locating files to upload")
filesinfo = locate_index_data(opts.input, opts[:index])
$logger.info("Got #{filesinfo[:data_files].length} data files to restore")

$logger.info("Checking index #{opts[:index]}")
indexinfo = check_index(opts.cluster, opts[:index], opts[:allow_overwrite])

$logger.info("Creating index #{opts[:index]}")
create_index(opts.cluster, opts[:index], filesinfo[:config])

strip_fields = opts.strip_fields.split(/\s*,\s*/)

$logger.info("Uploading data files")
p=1
filesinfo[:data_files].each { |datafile|
  $logger.info("#{p}/#{filesinfo[:data_files].length}: Uploading #{datafile}")
  upload_data_file(opts.cluster,opts[:index], datafile, strip_fields)
  p+=1
}
