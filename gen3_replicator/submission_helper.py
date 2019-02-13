from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

from graph_helper import db_credentials
from config_helper import load_json, config_path

# db passwords, etc.
creds = load_json('creds.json')
DEFAULT_ENDPOINT = 'https://{}'.format(creds['host'])
# DEFAULT_ENDPOINT = 'https://gen3.compbio.ohsu.edu'

# user's credentials
DEFAULT_CREDENTIALS_PATH = os.path.join(config_path(), 'credentials.json')
# DEFAULT_CREDENTIALS_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gen3.compbio.credentials.json')

if 'localhost' in DEFAULT_ENDPOINT:
    os.environ['CURL_CA_BUNDLE']=''
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def submission_client(endpoint=DEFAULT_ENDPOINT, refresh_file=DEFAULT_CREDENTIALS_PATH):
    auth = Gen3Auth(endpoint, refresh_file=refresh_file)
    assert auth , 'should return an auth client'
    submission_client = Gen3Submission(endpoint, auth)
    assert submission_client , 'should return a submission client'
    assert 'delete_program' in dir(submission_client), 'should have a delete_program method'
    assert 'create_program' in dir(submission_client), 'should have a create_program method'
    return submission_client
