"""Creates searchable index."""

import json
import inspect
from datetime import datetime
from collections import namedtuple

ReplicationRecord = namedtuple('ReplicationRecord', ['arrival_time', 'schema', 'table', 'action', 'key', 'keys', 'is_node', 'is_edge', 'clazz'])


class ModelMapper():
    """Map replication records to model. Map model to elastic."""

    def __init__(self, models):
        """Init state."""
        nodes, edges = self._map_tables(models)
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
        print(wal2json)
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

#
#
#
#     def transformer(self, msg):
#         """Transforms node to elastic document(s)."""
#         for replication_record in json_decode(msg.payload):
#             yield {
#                 '_index': label,
#                 '_type': label,
#                 '_id': make_query_id(_source, label, replication_record.config['keys']),
#                 '_source': _source
#             }
#
#
#     def json_decode(payload, models):
#         """Decode output from wal2json  replication output_plugin"""
#
#         # {
#         #   "change":[
#         #     {
#         #       "kind":"update",
#         #       "schema":"public",
#         #       "table":"node_project",
#         #       "columnnames":[
#         #         "created",
#         #         "acl",
#         #         "_sysan",
#         #         "_props",
#         #         "node_id"
#         #       ],
#         #       "columntypes":[
#         #         "timestamp with time zone",
#         #         "text[]",
#         #         "jsonb",
#         #         "jsonb",
#         #         "text"
#         #       ],
#         #       "columnvalues":[
#         #         "2019-01-18 16:36:04.325602+00",
#         #         "{}",
#         #         "{}",
#         #         "{\"code\": \"atac\", \"name\": \"atac\", \"state\": \"open\", \"availability_type\": \"Open\", \"dbgap_accession_number\": \"atac\"}",
#         #         "5272d000-f2da-510e-93b3-935d94c9415d"
#         #       ],
#         #       "oldkeys":{
#         #         "keynames":[
#         #           "node_id"
#         #         ],
#         #         "keytypes":[
#         #           "text"
#         #         ],
#         #         "keyvalues":[
#         #           "5272d000-f2da-510e-93b3-935d94c9415d"
#         #         ]
#         #       }
#         #     }
#         #   ]
#         # }
#
#
#         wal2json = json.loads(payload)
#         for change in wal2json['change']:
#             logging.debug(change)
#             if change['table'].startswith('node_'):
#
#             action = change['kind']
#             schema = change['schema']
#             table = change['table']
#             data = {'column_names': change['columnnames'],
#                     'column_types': change['columntypes'],
#                     'column_values': change['columnvalues'],
#                     'keys': change.get('oldkeys', change.get('keys',None))
#                     }
#             # key = '{}.{}'.format(schema, table)
#             # config = configs['queries'].get(key, {'is_passthrough': True})
#             arrival_time = datetime.now()
#             replication_record = ReplicationRecord(*(arrival_time, schema, table, action, data, config))
#             yield replication_record
#
#
#
#
#
# def make_id(replication_record):
#     """ id for replication_record in elastic """
#     label = make_label(replication_record)
#     if replication_record.data.get('keys', None):
#         id = '-'.join([ str(v) for v in replication_record.data['keys']['keyvalues'] ])
#         return '{}.{}'.format(label, id)
#     # hmm no keys, create a hash of values
#     m = hashlib.sha256()
#     for value in replication_record.data['column_values']:
#         m.update(str(value).encode('utf-8'))
#     return m.hexdigest()
