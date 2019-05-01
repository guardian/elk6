import os
from elasticsearch import Elasticsearch
from pprint import pprint,pformat
import json
import re
import datetime
import logging

filebeat_xtractor = re.compile(r'(filebeat|logstash)-(\d{4})\.(\d{2})\.(\d{2})')

logging.basicConfig(format="%(asctime)s [%(funcName)s] [%(levelname)s] - %(message)s", level=logging.DEBUG)

def find_filebeat_indices(es):
    data = es.cat.indices(format='json')
    logging.debug(pformat(data))

    for entry in data:
        parts = filebeat_xtractor.match(entry['index'])
        if parts:
            yield {
                'name': entry['index'],
                'timestamp': datetime.date(int(parts.group(2)),int(parts.group(3)),int(parts.group(4)))
            }
        else:
            logging.warning("Index {0} does not look like a filebeat index".format(entry['index']))

###START MAIN
def lambda_handler(event, context):
    eshost = os.environ["ES_HOST"]
    max_age = int(os.environ.get("MAX_AGE", 3)) #in days
    allow_delete = os.environ.get("ALLOW_DELETE", False)
    es = Elasticsearch(eshost)

    today = datetime.date.today()

    for info in find_filebeat_indices(es):
        pprint(info)
        age = today - info['timestamp']
        if age.days > max_age:
            if allow_delete:
                logging.info("I will delete {0}".format(info['name']))
                es.indices.delete(index=info['name'])
            else:
                logging.info("I would delete {0}, set ALLOW_DELETE in the environment to allow".format(info['name']))

if __name__=="__main__":
    lambda_handler(None, None)
