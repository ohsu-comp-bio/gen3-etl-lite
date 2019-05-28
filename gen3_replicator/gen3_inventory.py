"""Observe postgres and replicate to elastic."""

from __future__ import print_function
import sys
import psycopg2
import psycopg2.extras
import replication_helper
from model_helper import ModelMapper
from graph_helper import graph_connect
from flattener import flatten_row
from datetime import datetime, timedelta
from Queue import Queue
from threading import Thread
import time
from elastic_helper import bulk_upsert
from config_helper import save_json, load_json
import json
import sys

# key = label, value = [path_sql_query]
QUERIES = {}

def get_state():
    """Gets last_date processed by node type."""
    return load_json('state.json')


def save_state(state):
    """Saves last_date processed by node type."""
    save_json(state, 'state.json')


def get_index(observable_nodes, label):
    """Returns the category of the label."""
    for k in observable_nodes:
        if label in observable_nodes[k]:
            return observable_nodes[k][label]['category']


def query(graph, observable_nodes, model_mapper, sleep=100):
    """Transform and write expired records to elastic q."""
    LAST_DATE = '01/01/1900'
    while True:
        state = get_state()
        with graph.session_scope() as session:
            for k in observable_nodes:
                for label in observable_nodes[k]:
                    flattened_count = 0
                    index = get_index(observable_nodes, label)
                    last_date = state.get(label, LAST_DATE)
                    print(label, last_date)
                    # table_name = observable_nodes[k][label]['table_name']
                    for q  in QUERIES[label]:
                        q = q.replace('?',"'{}'".format(last_date))
                        column_names = {}
                        flattened_keys = {}
                        for r in session.execute(q):
                            column_names = r.keys()
                            flattened = flatten_row(r)
                            flattened_count += 1
                            flattened['_index'] = index
                            yield(flattened)
                            last_date = r[1]
                    state[label] = last_date
                    save_state(state)
                    print('{} wrote {} flattened_node(s) {}'.format(label, flattened_count, last_date))
                    sys.stdout.flush()
        print('sleep {}'.format(last_date))
        time.sleep(sleep)
    print('query_worker: done')


def elastic_worker(graph, observable_nodes, model_mapper):
    """Block, read q and write to elastic """
    def read_db():
        for flattened in query(graph, observable_nodes, model_mapper):
            if flattened:
                _op_type = 'index'
                _index = flattened['_index']
                _type = flattened['_index']
                del flattened['_index']
                bulk_input =  {
                    '_index': _index,
                    '_type': _type,
                    '_id': flattened['_node_id'],
                    '_source': flattened
                }
                # opcode defaults to index
                if 'is_delete' in flattened:
                    bulk_input['_op_type'] = 'delete'
                    _op_type = 'delete'
                # print('bulk_input {} {}.{} {}'.format(_op_type, bulk_input['_index'], bulk_input['_type'],  bulk_input['_id']), file=sys.stderr)
                yield bulk_input

    # write to elastic
    bulk_upsert(read_db)
    # while True:
    #     try:
    #         bulk_upsert(read_db)
    #     except Exception as e:
    #         print('bulk_upsert error {}'.format(str(e)), file=sys.stderr)
    #         raise e


def get_queries(observable_nodes):
    """Get all paths, for all nodes."""
    for k in observable_nodes:
        for label in observable_nodes[k]:
            path_queries = []
            with open('sql/{}.sql'.format(label), 'r') as sql_file:
                for line in sql_file:
                    path_queries.append(json.loads(line))
            print('path_queries {} {}'.format(label, len(path_queries)))
            QUERIES[label] = path_queries


def main():
    """Entrypoint."""

    # connect to graph, get model objects
    graph, models, observable_nodes  = graph_connect()

    # get all paths for observable_nodes
    get_queries(observable_nodes)


    print('Observing all changes to nodes {}'.format(observable_nodes.keys()), file=sys.stderr)
    model_mapper = ModelMapper(models)
    elastic_worker(graph, observable_nodes, model_mapper)


if __name__ == "__main__":
    main()
