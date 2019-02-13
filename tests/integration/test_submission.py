import uuid
import json
import pytest
import time
import requests

try:
    from  types import SimpleNamespace as SN
except ImportError as error:
    class SN (object):
        def __init__ (self, **kwargs):
            self.__dict__.update(kwargs)
        def __repr__ (self):
            keys = sorted(self.__dict__)
            items = ("{}={!r}".format(k, self.__dict__[k]) for k in keys)
            return "{}({})".format(type(self).__name__, ", ".join(items))
        def __eq__ (self, other):
            return self.__dict__ == other.__dict__

@pytest.fixture(scope="session")
def program_name():
    return 'smmart'


@pytest.fixture(scope="session")
def project_name():
    return 'atac'


@pytest.fixture(scope="session")
def case_name():
    return 'case-1'


@pytest.fixture(scope="session")
def sample_name():
    return 'sample-1'


@pytest.fixture(scope="session")
def aliquot_name():
    return 'aliquot-1'


@pytest.fixture(scope="session")
def submitted_methylation_name():
    return 'submitted_methylation'

    sample_name = 'sample-1'
    aliquot_name = 'aliquot-1'
    submitted_methylation_name = 'submitted_methylation-1'


def test_submission(submission_client):
    assert submission_client, 'should have a configured submission_client'


def test_list_projects(submission_client):
    q = '{ project { id, type, code } }'
    graph = submission_client.query(q)
    assert graph, 'should have a response to graphQL query'
    assert graph['data'], 'should have a data node {}'.format(graph)
    assert graph['data']['project'], 'should have a project(s) node {}'.format(graph)
    projects = list(map(lambda x: SN(**x), graph['data']['project']))
    assert len(projects), 'should have at least one project'
    assert projects[0].type == 'project', 'first element should be a project'
    print(projects)


def create_program(submission_client, program_name):
    program = SN(name=program_name, dbgap_accession_number=program_name, type='program').__dict__
    response = json.loads(submission_client.create_program(program))
    assert 'id' in response, 'could not create program {}'.format(response['message'])
    return response


def create_project(submission_client, program_name, project_name):
    project = SN(name=project_name,
        state="open", availability_type="Open",
        dbgap_accession_number=project_name, code=project_name, type='project').__dict__
    response = json.loads(submission_client.create_project(program_name, project))
    assert response['code']==200 , 'could not create project {}'.format(response['message'])
    return response


def create_node(submission_client, program_name, project_code, node):
    response = json.loads(submission_client.submit_node(program_name, project_code, node))
    assert response['code']==200 , 'could not create {} {}'.format(node['type'], response['message'])
    print('created {} {}'.format(response['entities'][0]['type'], response['entities'][0]['id']))
    return response



def create_experiment(submission_client, program_name, project_code, submitter_id):
    experiment = {
        '*projects': {'code': project_code},
        '*submitter_id': submitter_id,
        'type': 'experiment'
    }
    return create_node(submission_client, program_name, project_code, experiment)


def create_case(submission_client, program_name, project_code, submitter_id):
    case = {
        '*experiments': {'submitter_id': project_code},
        '*submitter_id': submitter_id,
        'type': 'case'
    }
    return create_node(submission_client, program_name, project_code, case)


def create_sample(submission_client, program_name, project_code, case_name, submitter_id):
    sample = {
        '*cases': {'submitter_id': case_name},
        '*submitter_id': submitter_id,
        'type': 'sample'
    }
    return create_node(submission_client, program_name, project_code, sample)


def create_aliquot(submission_client, program_name, project_code, sample_name, submitter_id):
    aliquot = {
        '*samples': {'submitter_id': sample_name},
        '*submitter_id': submitter_id,
        'type': 'aliquot'
    }
    return create_node(submission_client, program_name, project_code, aliquot)


def create_submitted_methylation(submission_client, program_name, project_code, aliquot_name, submitter_id):
    submitted_methylation = {
      "*data_category": 'Methylation Data',
      "*data_format": 'IDAT',
      "type": "submitted_methylation",
      "*submitter_id": submitter_id,
      "*data_type": 'Methylation Intensity Values',
      "*md5sum": '12345678901234567890123456789012',
      "*file_size": 1000,
      "aliquots": {
        "submitter_id": aliquot_name
      },
      'urls': 'foo',
      "*file_name": 'my-file-name',
    }
    return create_node(submission_client, program_name, project_code, submitted_methylation)


def delete_all(submission_client, program_name, project_name):

    types = ['submitted_methylation', 'aliquot', 'sample', 'case', 'experiment']
    for t in types:
        response = submission_client.export_node_all_type("smmart", "atac", t)
        if 'data' not in response:
            print('no data?', response)
        else:
            for n in response['data']:
                delete_response = json.loads(submission_client.delete_node(program_name, project_name, n['node_id']))
                assert delete_response['code'] == 200, delete_response
                print('deleted {} {}'.format(t, n['node_id']))


def create_all(submission_client, program_name, project_name, case_name, sample_name, aliquot_name, submitted_methylation_name):
    program = create_program(submission_client, program_name)
    print('created program {}'.format(program_name))
    project = create_project(submission_client, program_name, project_name)
    print('created project {}'.format(project_name))
    experiment = create_experiment(submission_client, program_name, project_name, submitter_id=project_name)
    case = create_case(submission_client, program_name, project_name, submitter_id=case_name)
    sample = create_sample(submission_client, program_name, project_name, case_name, submitter_id=sample_name)
    aliquot = create_aliquot(submission_client, program_name, project_name, sample_name, submitter_id=aliquot_name)
    submitted_methylation = create_submitted_methylation(submission_client, program_name, project_name, aliquot_name, submitter_id=submitted_methylation_name)


def test_delete_all(submission_client, program_name, project_name, elastic_host):
    delete_all(submission_client, program_name, project_name)
    print('waiting 15 secs to check replication')
    time.sleep(15)
    url = '{}/submitted_methylation/_search'.format(elastic_host)
    response = requests.get(url)
    assert response.status_code == 200, '{} should return 200 status'.format(url)
    assert response.json()['hits']['total'] == 0, 'should have 0 record {}'.format(url)
    url = '{}/aliquot/_search'.format(elastic_host)
    response = requests.get(url)
    assert response.status_code == 200, '{} should return 200 status'.format(url)
    assert response.json()['hits']['total'] == 0, 'should have 0 record {}'.format(url)
    print('replication OK')

def test_create_program_project(submission_client, program_name, project_name, case_name, sample_name, aliquot_name, submitted_methylation_name, elastic_host):
    create_all(submission_client, program_name, project_name, case_name, sample_name, aliquot_name, submitted_methylation_name)
    print('waiting 15 secs to check replication')
    time.sleep(15)
    url = '{}/submitted_methylation/_search'.format(elastic_host)
    response = requests.get(url)
    assert response.status_code == 200, '{} should return 200 status'.format(url)
    assert response.json()['hits']['total'] == 1, 'should have 1 record {}'.format(url)
    url = '{}/aliquot/_search'.format(elastic_host)
    response = requests.get(url)
    assert response.status_code == 200, '{} should return 200 status'.format(url)
    assert response.json()['hits']['total'] == 1, 'should have 1 record {}'.format(url)
    print('replication OK')
