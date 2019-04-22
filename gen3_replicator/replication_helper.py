"""Manage replication connection to postgres."""

from __future__ import print_function
from graph_helper import db_credentials
import psycopg2
import sys

DEFAULT_SLOT_NAME = 'pytest'


def replication_cursor(slot_name=DEFAULT_SLOT_NAME):
    """Connect to replication slot return (cursor, connection)."""
    connection_dict = db_credentials()
    connection_dict['connection_factory'] = psycopg2.extras.LogicalReplicationConnection
    conn = psycopg2.connect(**connection_dict)
    return conn.cursor(), conn


def drop_replication_slot(cur, slot_name=DEFAULT_SLOT_NAME):
    """Drop the replication slot if it exists."""
    try:
        cur.drop_replication_slot(slot_name)
    except psycopg2.ProgrammingError as e:
        pass


def start_replication(cur, slot_name=DEFAULT_SLOT_NAME, observable_nodes=None):
    """Start replication, create slot if not already there."""
    replication_parms = {'slot_name': slot_name, 'decode': True}
    # # notifications only for configured tables, see https://github.com/eulerto/wal2json#parameters
    if observable_nodes:
        table_names = []
        for k in observable_nodes.keys():
            for k2 in observable_nodes[k].keys():
                table_names.append(observable_nodes[k][k2]['table_name'])
            # add schema name
        replication_parms['options'] = {'add-tables': ','.join(['*.{}'.format(tn) for tn in table_names])}
    print('Started replication {}'.format(replication_parms), file=sys.stderr)
    try:
        cur.start_replication(**replication_parms)
    except psycopg2.ProgrammingError:
        cur.create_replication_slot(slot_name, output_plugin='wal2json')
        cur.start_replication(**replication_parms)
