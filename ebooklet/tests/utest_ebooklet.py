import pytest
import os, pathlib
import uuid6 as uuid
from tempfile import NamedTemporaryFile
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
from s3func import S3Session, HttpSession, B2Session
import booklet

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

try:
    with open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']
except:
    conn_config = {
        'service_name': 's3',
        'endpoint_url': os.environ['endpoint_url'],
        'aws_access_key_id': os.environ['aws_access_key_id'],
        'aws_secret_access_key': os.environ['aws_secret_access_key'],
        }


connection_config = conn_config
bucket = 'achelous'
flag = "n"
buffer_size = 524288
read_timeout = 60
threads = 10
file_name = 'gwrc_flow_sensor_datasets.csv'
file_path = script_path.joinpath(file_name)
db_key = uuid.uuid8().hex[:13]
db_key = 'ebooklet_test.blt'
file_path = script_path.joinpath(db_key)
# db_key = file_name
# remote_db_key = 'gwrc_flow_sensor_sites.gpkg'
# remote_db_key = '802037bfcd1c47359b36affdc893c7cc'
key = 'gwrc_flow_sensor_datasets.csv'
base_url = 'https://b2.tethys-ts.xyz/file/' + bucket + '/'
remote_url = base_url +  db_key
db_url = remote_url
value_serializer = 'pickle_zstd'
retries: int=3
object_lock=False
break_other_locks=False
init_remote=True
lock_timeout=-1
local_storage_kwargs = {}
n_buckets = 12007

# s3 = s3.client(conn_config)
d

################################################
### Pytest stuff


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    rep = outcome.get_result()

    # set a report attribute for each phase of a call, which can
    # be "setup", "call", "teardown"

    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture
def get_logs(request):
    yield

    if request.node.rep_call.failed:
        # Add code here to cleanup failure scenario
        print("executing test failed")

        obj_keys = []
        for js in list_object_versions(s3, bucket):
            if js['Key'] == obj_key:
                obj_keys.append({'Key': js['Key'], 'VersionId': js['VersionId']})

        if obj_keys:
            delete_objects(s3, bucket, obj_keys)

    # elif request.node.rep_call.passed:
    #     # Add code here to cleanup success scenario
    #     print("executing test success")


################################################
### Tests

s3_remote = remotes.S3Remote(db_key, bucket, connection_config)
http_remote = remotes.HttpRemote(db_url)

remote = [s3_remote, http_remote]

self = EBooklet(
    remote,
    file_path,
    flag,
    value_serializer,
    buffer_size=524288,
    )

self = EBooklet(
    remote,
    file_path,
    'c',
    )

self['test1'] = list(range(100))
self['test2'] = 10

changes = self.changes()

changes.update()

list(changes.iter_changes())

changes.discard()

changes.push()
changes.pull()

self = EBooklet(
    remote,
    file_path,
    'w',
    )

self['test3'] = 999

if self._remote_index is not None:
    print('true')

if s3_remote.uuid:
    print('true')


self = EBooklet(
    [s3_remote, http_remote],
    file_path,
    'r',
    )


with open(self._remote_index_path, 'rb') as ri:
    b1 = ri.read()


self = booklet.VariableValue(file_path)
















































def test_put_object():
    """

    """
    ### Upload with bytes
    with open(script_path.joinpath(file_name), 'rb') as f:
        obj = f.read()

    resp1 = put_object(s3, bucket, obj_key, obj)

    meta = resp1['ResponseMetadata']
    if meta['HTTPStatusCode'] != 200:
        raise ValueError('Upload failed')

    resp1_etag = resp1['ETag']

    ## Upload with a file-obj
    resp2 = put_object(s3, bucket, obj_key, open(script_path.joinpath(file_name), 'rb'))

    meta = resp2['ResponseMetadata']
    if meta['HTTPStatusCode'] != 200:
        raise ValueError('Upload failed')

    resp2_etag = resp2['ETag']

    assert resp1_etag == resp2_etag


def test_list_objects():
    """

    """
    count = 0
    found_key = False
    for i, js in enumerate(list_objects(s3, bucket)):
        count += 1
        if js['Key'] == obj_key:
            found_key = True

    assert found_key


def test_list_object_versions():
    """

    """
    count = 0
    found_key = False
    for i, js in enumerate(list_object_versions(s3, bucket)):
        count += 1
        if js['Key'] == obj_key:
            found_key = True

    assert found_key


def test_get_object():
    """

    """
    stream1 = get_object(obj_key, bucket, s3)
    data1 = stream1.read()

    stream2 = get_object(obj_key, bucket, connection_config=conn_config)
    data2 = stream2.read()

    assert data1 == data2


def test_url_to_stream():
    """

    """
    stream1 = url_to_stream(url)
    data1 = stream1.read()

    stream2 = base_url_to_stream(obj_key, base_url)
    data2 = stream2.read()

    assert data1 == data2


def test_legal_hold():
    """

    """
    hold = get_object_legal_hold(s3, bucket, obj_key)
    if hold:
        raise ValueError("There's a hold, but there shouldn't be.")

    put_object_legal_hold(s3, bucket, obj_key, True)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if not hold:
        raise ValueError("There isn't a hold, but there should be.")

    put_object_legal_hold(s3, bucket, obj_key, False)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if hold:
        raise ValueError("There's a hold, but there shouldn't be.")

    resp2 = put_object(s3, bucket, obj_key, open(script_path.joinpath(file_name), 'rb'), object_legal_hold=True)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if not hold:
        raise ValueError("There isn't a hold, but there should be.")

    put_object_legal_hold(s3, bucket, obj_key, False)

    hold = get_object_legal_hold(s3, bucket, obj_key)
    if hold:
        raise ValueError("There's a hold, but there shouldn't be.")

    assert True


def test_delete_objects():
    """

    """
    obj_keys = []
    for js in list_object_versions(s3, bucket):
        if js['Key'] == obj_key:
            obj_keys.append({'Key': js['Key'], 'VersionId': js['VersionId']})

    delete_objects(s3, bucket, obj_keys)

    found_key = False
    for i, js in enumerate(list_object_versions(s3, bucket)):
        if js['Key'] == obj_key:
            found_key = True

    assert not found_key









































































