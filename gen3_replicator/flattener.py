from __future__ import print_function
import sys
from model_helper import name


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


def flatten_delete(replication_record):
    """Creates a flat record for delete, None otherwise."""
    if replication_record.action == 'delete':
        return {
            'node_id': replication_record.key,
            'is_delete': True,
            'label': replication_record.clazz.label
        }
    return None


def flatten_node(node):
    """Traverses node renders a flat object."""
    flat = {}
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
                # skip system properties if not root node
                if k in ['project_id', 'id', 'node_id']:
                    continue
                # skip datetime until guppy supports
                if 'datetime' in k:
                    continue
                flat['{}_{}'.format(node.label, k)] = node.properties[k]
        return flat
    traverse_up(node, flatten_node)
    # traverse down, this node already rendered
    for e in node.edges_in:
        traverse_down(e.src, flatten_node)
    return flat


def flatten_row(row):
    """Given a query result row, flatten it."""
    # [u'hop_survey.node_id', u'hop_survey.created', u'hop_survey',
    # u'case.node_id', u'case',
    # u'experiment.node_id', u'experiment',
    # u'project.node_id', u'project',
    # u'program.node_id', u'program']
    label_created_col_name = [k for k in row.keys() if 'created' in k][0]
    label_object_col_name = label_created_col_name.split('.')[0]
    label_node_id_col_name = '{}.node_id'.format(label_object_col_name)

    flattened = row[label_object_col_name]
    flattened['_created'] = row[label_created_col_name]
    flattened['_node_id'] = row[label_node_id_col_name]

    path_members = [k for k in row.keys() if 'node_id' not in k and label_object_col_name not in k]
    for path_member in path_members:
        for k in row[path_member]:
            flattened['{}_{}'.format(path_member, k)] = row[path_member][k]
        flattened['{}_{}'.format(path_member, 'node_id')] = row['{}.node_id'.format(path_member)]

    return flattened

def flatten_aliquot(node):
    """Traverses node renders a flat object."""
    flat = {}
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
                # skip system properties if not root node
                if k in ['project_id', 'id', 'node_id']:
                    continue
                # skip datetime until guppy supports
                if 'datetime' in k:
                    continue
                flat['{}_{}'.format(node.label, k)] = node.properties[k]

    traverse_up(node, flatten_node)
    for bcc_sample in node.sample.bcc_sample:
        flatten_node(bcc_sample)
    for diagnosis in node.sample.diagnosis:
        flatten_node(diagnosis)
        for treatment in diagnosis.treatment:
            flatten_node(treatment)
            for bcc_chemotherapy in treatment.bcc_chemotherapy:
                flatten_node(bcc_chemotherapy)


    # traverse down, this node already rendered
    for e in node.edges_in:
        traverse_down(e.src, flatten_node)
    return flat
