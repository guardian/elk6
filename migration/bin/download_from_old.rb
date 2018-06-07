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
  opt :index, "Download this index", :type=>:string
  opt :output, "Output json files to this directory", :type=>:string,:default=>Dir.pwd
  opt :chunksize, "Get this many records per json file", :type=>:integer, :default=>1000
end

def check_cluster(address)
  $logger.debug("Connecting to #{address}...")
  uri = URI(address)
  raw_json = Net::HTTP.get(uri)
  content = JSON.parse(raw_json)
  $logger.info("Connected to #{content['cluster_name']}, ES version #{content['version']['number']}. Status is #{content['status']}")
  raise RuntimeError, "Server is in an error state" if content['status'] != 200
end #def check_cluster

def check_index(address, indexname)
  uri = URI(File.join(address,indexname))
  $logger.debug("Connecting to #{uri}...")
  response = Net::HTTP.get_response(uri)
  content = JSON.parse(response.body)
  if response.code != '200'
    ap content
    raise RuntimeError, "Could not access index #{indexname}, server returned a #{response.code} error"
  end
  $logger.info("Got information about index #{indexname}")
  content
end

def download_next_page(http,address,indexname, outputdir, page_number, page_size)
  #returns TRUE if more results, otherwise FALSE
  from = page_number * page_size
  uri = URI("#{address}/#{indexname}/_search?from=#{from}&size=#{page_size}")
  $logger.debug("Connecting to #{uri}...")
  request = Net::HTTP::Post.new(uri)
  response = http.request request

  if response.code != '200'
    $logger.error(response.body)
    raise RuntimeError, "Could not download page data, server returned #{response.code} error"
  end

  content = JSON.parse(response.body)
  outfile = File.join(outputdir,"indexdata-#{indexname}-#{page_number}.json")
  expected_pages = content['hits']['total']/page_size
  $logger.info("Writing page #{page_number} (out of expected #{expected_pages}) to #{outfile}")
  open outfile,"w" do |f|
    f.write(response.body)
  end #open outfile
  return content['hits']['hits'].length==page_size
end #def download_next_page

#START MAIN

#Establish connection
$logger.info("Starting up")
check_cluster(opts.cluster)

#Get index setup
$logger.info("Checking index #{opts[:index]}")
indexinfo = check_index(opts.cluster, opts[:index])
ap indexinfo

outpath = File.join(opts.output,"indexinfo-#{opts[:index]}.json")
$logger.info("Outputting to #{outpath}")
open outpath, "w" do |f|
  f.write(JSON.generate(indexinfo))
end #open

page_number=0

search_uri = URI("#{opts.cluster}/#{opts[:index]}/_search")
Net::HTTP.start(search_uri.host, search_uri.port) do |http|
  more_hits=true
  while more_hits
    more_hits=download_next_page(http, opts.cluster, opts[:index], opts.output, page_number, opts.chunksize)
    ap more_hits
    page_number+=1
  end #while True
end #Net::HTTP.start

$logger.info("Completed, dumped #{page_number} pages of results")
