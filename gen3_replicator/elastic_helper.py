"""Manage elasticsearch."""

import os
import ssl
from elasticsearch.connection import create_ssl_context
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch.helpers import bulk
import urllib3


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

ELASTICSEARCH_URL = os.environ.get('ELASTICSEARCH_URL', "http://esproxy-service")

def bulk_upsert(document_generator, elasticsearch_url=ELASTICSEARCH_URL):
    """Connects to ELASTIC_HOST, reads elastic documents from generator,  writes objects to elastic."""
    client = connect()
    bulk(client,
        (d for d in document_generator()),
        request_timeout=120)


def connect(elasticsearch_url=ELASTICSEARCH_URL):
    # client = Elasticsearch([elasticsearch_url])
    ssl_context = create_ssl_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    return Elasticsearch(elasticsearch_url, ssl_context=ssl_context)
