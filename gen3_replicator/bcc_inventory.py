from __future__ import print_function
import sys
import psycopg2
import psycopg2.extras
import replication_helper
from model_helper import ModelMapper
from graph_helper import graph_connect
from datetime import datetime, timedelta
from Queue import Queue
from threading import Thread
import time
from elastic_helper import bulk_upsert
import resource

graph, models, observable_nodes  = graph_connect()

IGNORED = ['created_datetime', 'node_id', 'updated_datetime', 'submitter_id', 'project_id']

def strip_ignored(properties):
    return {k:v for k,v in properties.items() if k not in IGNORED}

def traverse_participant(p, node):
    for k,v in strip_ignored(p.properties).items():
        node[k] = v
    return p


def biomarker_transform(props):
    if 'biomarker_level' in props:
        props['biomarker_level'] = float(props['biomarker_level'])
    return props

def traverse_biomarker(b, node):
    if 'biomarker' not in node:
        node['biomarker'] = []
    node['biomarker'].append(biomarker_transform(strip_ignored(b.properties)))
    return b


def traverse_aliqot(a, node):
    if len(a.genetrails_variant) == 0:
        # print('no genetrails_variant')
        return a
    node['genetrails_variant'] = [strip_ignored(gv.properties) for gv in a.genetrails_variant]
    return a


def traverse_sample(s, node):
    if len(s.bcc_sample) == 0:
        print('no bcc_sample')
        return s
    node['sample'] = [strip_ignored(bs.properties) for bs in s.bcc_sample]
    # TODO skip for now
    # [traverse_aliqot(a, node['sample'] ) for a in s.aliquots]
    return s


def traverse_observation(o, node):
    if 'observations' not in node:
        node['observations'] = {'bcc_lesion': [], 'bcc_weight': []}
    for bd in o.bcc_lesion:
        node['observations']['bcc_lesion'].append(strip_ignored(bd.properties))
    for bd in o.bcc_weight:
        node['observations']['bcc_weight'].append(strip_ignored(bd.properties))
    return o


def traverse_diagnosis(d, node):
    node['diagnoses'] = [strip_ignored(bd.properties) for bd in d.bcc_diagnosis][0]
    [traverse_treatment(t, node) for t in d.treatments]
    return d


def traverse_demographic(d, node):
    node['demographics'] = [strip_ignored(bd.properties) for bd in d.bcc_demographic][0]
    return d


def traverse_treatment(t, node):
    node['treatments'] = []
    for c in [t.bcc_surgery, t.bcc_chemotherapy, t.bcc_radiotherapy]:
        for p in c:
            node['treatments'].append(strip_ignored(p.properties))
    return t


def traverse_case(c):
    node = dict(c.properties)
    node['node_id'] = c.node_id
    traversal_counts = {
        'observations':  len([traverse_observation(o, node) for o in c.observations]),
        'diagnosis':  len([traverse_diagnosis(d, node) for d in c.diagnoses]),
        'participants': len([traverse_participant(p, node) for p in c.bcc_participants]),
        'biomarkers': len([traverse_biomarker(b, node) for b in c.bcc_biomarkers]),
        'demographics': len([traverse_demographic(d, node) for d in c.demographics]),
        'samples': len([traverse_sample(s, node) for s in c.samples]),
    }
    assert traversal_counts
    return node


def all_cases(last_date=None, ids=None):
    with graph.session_scope() as session:
        if not ids:
            q = "select node_id from node_case where _props->>'project_id' =  'ohsu-bcc' and created > '{}'".format(last_date)
            ids = [r[0] for r in session.execute(q)]
        # ids = ['09ccbde5-2a9d-5d0c-bea7-254290e460f9']
        cases = graph.nodes(models.Case).ids(ids).all()
        for c in cases:
            print(c.node_id,  resource.getrusage(resource.RUSAGE_SELF).ru_maxrss)
            yield traverse_case(c)
            # evict from session now that we are done with it
            session.expire(c)


def to_elastic(last_date=None, ids=None):
    def make_bulk():
        for case in all_cases(last_date, ids=ids):
            yield {
                '_index': 'case',
                '_type': 'case',
                '_id': case['node_id'],
                '_source': case
            }
    bulk_upsert(make_bulk)



def main():
    """Entrypoint."""
    to_elastic(last_date='01/01/1900')


if __name__ == "__main__":
    main()
