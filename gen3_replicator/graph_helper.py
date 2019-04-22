"""Connects to gen3 graph after reading config."""
from __future__ import print_function
import datamodelutils
from dictionaryutils import DataDictionary, dictionary
from gdcdictionary import gdcdictionary
from gdcdatamodel import models, validators
import os
from psqlgraph import PsqlGraphDriver
from sqlalchemy import * # noqa
from config_helper import load_json
import inspect
from flattener import flatten


def graph_connect():
    """Load config and connect to graph, return graph and models."""
    if ('DICTIONARY_URL' in os.environ):
        url = os.environ['DICTIONARY_URL']
        datadictionary = DataDictionary(url=url)
    elif ('PATH_TO_SCHEMA_DIR' in os.environ):
        datadictionary = DataDictionary(root_dir=os.environ['PATH_TO_SCHEMA_DIR'])
    else:
        datadictionary = gdcdictionary.gdcdictionary

    dictionary.init(datadictionary)
    datamodelutils.validators.init(validators)
    datamodelutils.models.init(models)
    graph = PsqlGraphDriver(**db_credentials())
    return graph, models, observable_nodes(dictionary, models)


def observable_nodes(dictionary, models):
    """return {index_name: {table_name, node}} for all nodes we are interested in."""
    indexes = {}
    my_models = {}
    for t in inspect.getmembers(models, inspect.isclass):
        if hasattr(t[1], '__tablename__') and hasattr(t[1], 'node_id') and hasattr(t[1], 'label'):
            my_models[t[1].label] = t[1].__tablename__
    whitelist_nodes  = {}
    file_tuples = [(table_name, node) for table_name, node in dictionary.schema.iteritems() if node['category'] == 'data_file']
    for t in file_tuples:
        whitelist_nodes[t[0]] = t[1]
        whitelist_nodes[t[0]]['table_name'] = my_models[t[0]]

    indexes['files'] = whitelist_nodes

    indexes['aliquots'] = {'aliquot': dictionary.schema['aliquot']}
    indexes['aliquots']['aliquot']['table_name'] = my_models['aliquot']
    
    return indexes


def db_credentials():
    """Load creds and returns dict of postgres keyword arguments."""
    creds = load_json('creds.json')
    return {
        'host': creds['db_host'],
        'user': creds['db_username'],
        'password': creds['db_password'],
        'database': creds['db_database']
    }
