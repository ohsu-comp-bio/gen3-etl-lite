from __future__ import print_function
import sys

def traverse_down(node, node_callback):
    """Recursively process the node using node_callback(node)."""
    node_callback(node)
    for e in node.edges_in:
        traverse_down(e.src, node_callback)


def traverse_up(node, node_callback):
    """Recursively process the node using node_callback(node)."""
    node_callback(node)
    for e in node.edges_out:
        traverse_up(e.dst, node_callback)

def get_index(observable_nodes, label):
    """Returns the category of the label."""
    for k in observable_nodes:
        if label in observable_nodes[k]:
            return observable_nodes[k][label]['category']


def flatten(graph, replication_record, observable_nodes):
    """Fetch node from replication record, traverse it, flatten to an object."""
    index = get_index(observable_nodes, replication_record.clazz.label)
    flat = {}
    print('flatten {} {} {}'.format(replication_record.action,replication_record.clazz.label,  replication_record.key), file=sys.stderr)
    if replication_record.action == 'delete':
        flat['node_id'] = replication_record.key
        flat['is_delete'] = True
        flat['label'] = replication_record.clazz.label
        flat['_index'] = index
        return flat

    def flatten_node(node):
        """Replicate the node."""
        if 'node_id' not in flat:
            flat['node_id'] = node.node_id
            flat['label'] = node.label
            if 'project_id' in node.properties:
                program, project = node.properties['project_id'].split('-')
                flat['gen3_resource_path'] = '/programs/{}/{}'.format(program, project)
            # creates flat document
            for k in node.properties.keys():
                # skip datetime until guppy supports
                if 'datetime' in k:
                    continue
                flat[k] = node.properties[k]
        else:
            # creates flat document
            for k in node.properties.keys():
                # skip datetime until guppy
                if 'datetime' in k:
                    continue
                flat['{}_{}'.format(node.label, k)] = node.properties[k]
            flat['{}_{}'.format(node.label, 'node_id')] = node.node_id

        # # creates nested document
        # parent =  {}
        # parent['node_id'] = node.node_id
        # parent['label'] = node.label
        # for k in node.properties.keys():
        #     parent[k] = node.properties[k]
        # flat[node.label] = parent


    with graph.session_scope():
        # print(replication_record.clazz, replication_record.key)
        traverse_up(graph.nodes(replication_record.clazz).ids(replication_record.key).one(), flatten_node)

    flat['_index'] = index
    return flat
