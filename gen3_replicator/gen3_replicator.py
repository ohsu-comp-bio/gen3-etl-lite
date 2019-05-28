"""Observe postgres and replicate to elastic."""

from __future__ import print_function
import sys
import psycopg2
import psycopg2.extras
import replication_helper
from model_helper import ModelMapper
from graph_helper import graph_connect, flatten
from datetime import datetime, timedelta
from Queue import Queue
from threading import Thread
import time
from elastic_helper import bulk_upsert

class PrintingConsumer(object):
    """Consume replication messages."""

    def __init__(self, models):
        self.model_mapper = ModelMapper(models)

    def __call__(self, msg):
        """Process callback from consume_stream."""
        for model in self.model_mapper.get_models(msg.payload):
            print(model)
        # TODO more efficient not to feedback every message
        msg.cursor.send_feedback(flush_lsn=msg.data_start)


class ReplicatingConsumer(object):
    """Consume replication messages."""

    def __init__(self, models, expiry=10):
        self.model_mapper = ModelMapper(models)
        # hold the latest ReplicationRecord for table until N seconds expire, then schedule harvest of data
        # key node_id
        # value ReplicationRecord
        self.pending = {}
        self.expiry = expiry

    def __call__(self, msg):
        """Process callback from consume_stream."""
        for model in self.model_mapper.get_models(msg.payload):
            self.pending[model.key] = model
        # TODO more efficient not to feedback every message
        msg.cursor.send_feedback(flush_lsn=msg.data_start)

    def expired_records(self):
        """Any has older than expired?"""
        expired =  datetime.now() - timedelta(seconds=self.expiry)
        # find them
        delete_from_pending = []
        for k in self.pending.keys():
            replication_record = self.pending[k]
            if replication_record.arrival_time < expired:
                delete_from_pending.append( (k, replication_record) )
        # delete them
        for deletable in delete_from_pending:
            del self.pending[deletable[0]]
        # process them
        for k, replication_record in delete_from_pending:
            #print('expired_records {} {} {}'.format(replication_record.action, replication_record.clazz.label, replication_record.key), file=sys.stderr)
            yield replication_record


def get_index(observable_nodes, label):
    """Returns the category of the label."""
    for k in observable_nodes:
        if label in observable_nodes[k]:
            return observable_nodes[k][label]['category']


def query_worker(replicating_consumer, graph, elastic_q, observable_nodes, sleep=1):
    """Transform and write expired records to elastic q."""
    while True:
        for expired_record in replicating_consumer.expired_records():
            index = get_index(observable_nodes, expired_record.clazz.label)
            flattened_node = flatten_delete(expired_record, index)
            if not flattened_node:
                with graph.session_scope():
                    flattened_node = flatten(graph.nodes(expired_record.clazz).ids(expired_record.key).one())
            flattened_node['_index'] = index
            elastic_q.put(flatten(graph, expired_record, observable_nodes))
        time.sleep(sleep)


def elastic_worker(elastic_q):
    """Block, read q and write to elastic """
    def read_queue():
        flattened = elastic_q.get()
        if flattened:
            _op_type = 'index'
            _index = flattened['_index']
            _type = flattened['_index']
            del flattened['_index']
            bulk_input =  {
                # delete records won't have a payload that indicates project_id, use wildcard
                # '_index': '{}_{}'.format(_finditem(flattened, 'project_id', '*'), flattened['label']),
                '_index': _index,
                '_type': _type,
                '_id': flattened['node_id'],
                '_source': flattened
            }
            # opcode defaults to index
            if 'is_delete' in flattened:
                bulk_input['_op_type'] = 'delete'
                _op_type = 'delete'
            # print('bulk_input {} {}.{} {}'.format(_op_type, bulk_input['_index'], bulk_input['_type'],  bulk_input['_id']), file=sys.stderr)
            yield bulk_input

    def _finditem(obj, key, default):
        """simple function to recursively search and return key value """
        if key in obj: return obj[key]
        for k, v in obj.items():
            if isinstance(v,dict):
                return _finditem(v, key)
        return default

    # write to elastic
    while True:
        try:
            bulk_upsert(read_queue)
        except Exception as e:
            print('bulk_upsert error {}'.format(str(e)), file=sys.stderr)



def main():
    """Entrypoint."""

    # connect to graph, get model objects
    graph, models, observable_nodes  = graph_connect()

    print('Observing all changes to nodes {}'.format(observable_nodes.keys()), file=sys.stderr)

    # connect to db, get stream of replication events
    cur, conn = replication_helper.replication_cursor()
    replication_helper.drop_replication_slot(cur)
    replication_helper.start_replication(cur, observable_nodes=observable_nodes)

    # queues
    elastic_q = Queue(maxsize=0)

    # setup consumer
    replicating_consumer = ReplicatingConsumer(models)

    # start worker threads
    threads = []
    worker = Thread(target=query_worker, args=(replicating_consumer, graph, elastic_q, observable_nodes), name='query_worker')
    worker.setDaemon(True)
    worker.start()
    threads.append(worker)

    worker = Thread(target=elastic_worker, args=(elastic_q,), name='elastic_worker')
    worker.setDaemon(True)
    worker.start()
    threads.append(worker)


    print("Starting streaming, press Control-C to end...", file=sys.stderr)
    try:
        cur.consume_stream(replicating_consumer)
        logging.debug('waiting')
        elastic_q.join()
        for t in threads:
            t.join()
        logging.debug('done')

    except KeyboardInterrupt:
        cur.close()
        conn.close()
        print("The slot slot_name still exists. Drop it with "
              "SELECT pg_drop_replication_slot(slot_name); if no longer needed.",
              file=sys.stderr)
        print("WARNING: Transaction logs will accumulate in pg_xlog "
              "until the slot is dropped.", file=sys.stderr)


if __name__ == "__main__":
    main()
