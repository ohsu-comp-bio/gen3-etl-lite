"""Manage elasticsearch."""

import os
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search
from elasticsearch.helpers import bulk

ELASTIC_HOST = os.environ.get('ELASTIC_HOST', "http://esproxy-service")

def bulk_upsert(document_generator, elastic_host=ELASTIC_HOST):
    """Connects to ELASTIC_HOST, reads elastic documents from generator,  writes objects to elastic."""
    client = Elasticsearch([elastic_host])
    bulk(client,
        (d for d in document_generator()),
        request_timeout=120)
