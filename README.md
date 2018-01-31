# ELK6 stack

This project is an amazon-hosted ELK stack for version 6.  It uses the existing AWS ElasticSearch service
and creates nodes for Kibana and Logstash.

## How to use it
### Requirements

- An AWS account (duh!), with at least one VPC configured
- packer (https://www.packer.io/) on your local machine
- Ruby 2.x on your local machine.  Macs should have this by default, unless they
are really old, if you're on Linux and you don't already have it then it should be
simple enough to install from your favourite package management system
- AWS credentials for Packer, normally using environment variables or profiles

### Step one - build the images

- Check out this repo to your local machine
- Make sure that you set up credentials for your AWS account in a Terminal session
- Then you should be able to build the Logstash image by running `utils/build-image.rb logstash` from the repo directory
- Make a note of the AMI ID that it has created
- Now build the Kibana image by running `utils/build-image.rb kibana` from the repo directory
- Again, make a note of the AMU ID that it has created
- **Note** - don't try to build the two in parallel, the build-image script creates a tempfile for packer that may get over-written if you try

### Step two - deploy the stack

- Log in to the AWS Console and go to Cloud Formation
- Click Create Stack and upload `cloudformation/fullstack.yaml` from the repo
- Most of the parameters should be self-explanatory.  **Note** - you must specify *exactly two* subnets to deploy into, because of how Amazon's ElasticSearch service works.
- Paste in the AMI IDs of the Logstash and Kibana images that you created

### Step three - use it

- Install Filebeat or similar onto the nodes you want to monitor - see https://www.elastic.co/products/beats for more information
- Use the Logstash endpoint as the target for Filebeat etc. to push to
- Set up a DNS CNAME to get a more usable Kibana endpoint than the auto-generated one
- The Logstash and Kibana endpoints are shown as CloudFormation outputs and they are exported.  You can either use these directly in your Beats configs (not recommended) or import them as external references into another CloudFormation stack (see https://docs.aws.amazon.com/AWSCloudFormation/latest/UserGuide/walkthrough-crossstackref.html) and pass that value into your Beats config.
