"""Creates searchable index."""
from __future__ import print_function
import json
import inspect
from datetime import datetime
from collections import namedtuple
from config_helper import load_json

ReplicationRecord = namedtuple('ReplicationRecord', ['arrival_time', 'schema', 'table', 'action', 'key', 'keys', 'is_node', 'is_edge', 'clazz'])


class ModelMapper():
    """Map replication records to model. Map model to elastic."""

    def __init__(self, models):
        """Init state."""
        nodes, edges= self._map_tables(models)
        self.nodes = nodes
        self.edges = edges

    @classmethod
    def _map_tables(cls, models):
        """Return nodes and edges key:table, val:class."""
        ModelTable = namedtuple('ModelTable', ['tablename', 'clazz'])
        orm_objects = [ModelTable(t[1].__tablename__, t[1]) for t in inspect.getmembers(models, inspect.isclass) if hasattr(t[1], '__tablename__') ]
        nodes = {}
        for n in orm_objects:
            if hasattr(n.clazz, 'node_id'):
                nodes[n.tablename] = n.clazz
        edges = {}
        for e in orm_objects:
            if hasattr(e.clazz, 'src_id'):
                edges[e.tablename] = e.clazz
        return nodes, edges

    def get_models(self, payload):
        """Iterate replication record(s) from msg that includes the model class"""
        wal2json = json.loads(payload)
        for change in wal2json['change']:
            action = change['kind']
            schema = change['schema']
            table = change['table']
            keys = change.get('oldkeys', change.get('keys',None))
            if not keys:
                for n,v in zip(change['columnnames'], change['columnvalues']):
                    if n == 'node_id':
                        key = v
            else:
                key = keys['keyvalues'][0]
            arrival_time = datetime.now()
            is_node = table in self.nodes
            is_edge = table in self.edges
            clazz = None
            if is_node:
                clazz = self.nodes[table]
            if is_edge:
                clazz = self.edges[table]
            replication_record = ReplicationRecord(*(arrival_time, schema, table, action, key, keys, is_node, is_edge, clazz))
            yield replication_record


def name(object):
    """Returns the name of a given object ala rails' :name."""
    alias = get_name_alias(object)
    return object[alias]

NAME_ALIASES = load_json('name_aliases.json')

def get_name_alias(object):
    """Returns the attribute that contains human readable text."""
    t = object.get('type', None)
    if t in NAME_ALIASES:
        return NAME_ALIASES[t]
    return 'submitter_id'
