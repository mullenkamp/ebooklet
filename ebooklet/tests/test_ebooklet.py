import pytest
import os, pathlib, io
import uuid6 as uuid
from tempfile import NamedTemporaryFile
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
from s3func import S3Session, HttpSession, B2Session
import booklet
import ebooklet
from ebooklet import __version__, remote, RemoteConnGroup, EVariableLengthValue
from copy import deepcopy

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

try:
    with io.open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']

    endpoint_url = conn_config['endpoint_url']
    access_key_id = conn_config['access_key_id']
    access_key = conn_config['access_key']
except:
    endpoint_url = os.environ['endpoint_url']
    access_key_id = os.environ['access_key_id']
    access_key = os.environ['access_key']

# tf = NamedTemporaryFile()
# file_path = tf.name

# connection_config = conn_config

bucket = 'achelous'
flag = "n"
buffer_size = 524288
read_timeout = 60
threads = 10

db_key = uuid.uuid8().hex[-13:]
db_key2 = uuid.uuid8().hex[-13:]
# db_key = 'test.blt'
# db_key2 = 'test2.blt'

file_path = script_path.joinpath(db_key)
base_url = 'https://b2.tethys-ts.xyz/file/' + bucket + '/'
db_url = base_url +  db_key
value_serializer = 'pickle'
remote_object_lock=False
file_path_rcg = pathlib.Path(str(file_path) + '.rcg')
db_key_rcg = db_key + '.rcg'
db_url_rcg = db_url + '.rcg'

data_dict = {str(key): key*2 for key in range(2, 30)}

data = deepcopy(data_dict)

meta = {'test1': 'data'}



################################################
### Tests

print(__version__)

remote_conn = remote.S3Connection(access_key_id, access_key, db_key, bucket, endpoint_url=endpoint_url, db_url=db_url)

remote_conn2 = remote.S3Connection(access_key_id, access_key, db_key2, bucket, endpoint_url=endpoint_url)

# http_conn = remote.HttpConn(db_url)

# remote_conn = remote.Conn(s3_conn=s3_conn, http_conn=http_conn)


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


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup a test data."""
    def remove_test_data():
        with remote_conn.open('w') as s3open:
            s3open.delete_remote()

        file_path.unlink()
        remote_index_path = file_path.parent.joinpath(file_path.name + '.remote_index')
        remote_index_path.unlink()

    request.addfinalizer(remove_test_data)


# @pytest.fixture
# def get_logs(request):
#     yield

#     if request.node.rep_call.failed:
#         # Add code here to cleanup failure scenario
#         print("executing test failed")

#         with remote_conn.open('w') as s3open:
#             s3open.delete_remote()

#         file_path.unlink()
#         remote_index_path = file_path.parent.joinpath(file_path.name + '.remote_index')
#         remote_index_path.unlink()


    # elif request.node.rep_call.passed:
    #     # Add code here to cleanup success scenario
    #     print("executing test success")

################################################
### Normal local operations


def test_set_items():
    with ebooklet.open(remote_conn, file_path, 'n', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with ebooklet.open(remote_conn, file_path) as f:
        value = f['10']

    assert value == data_dict['10']


def test_update():
    with ebooklet.open(remote_conn, file_path, 'n', value_serializer='pickle') as f:
        f.update(data_dict)

    with ebooklet.open(remote_conn, file_path) as f:
        value = f['10']

    assert value == data_dict['10']


def test_set_get_metadata():
    """

    """
    with ebooklet.open(remote_conn, file_path, 'w') as f:
        old_meta = f.get_metadata()
        f.set_metadata(meta)

    assert old_meta is None

    with ebooklet.open(remote_conn, file_path) as f:
        new_meta = f.get_metadata()

    assert new_meta == meta


def test_set_get_timestamp():
    with ebooklet.open(remote_conn, file_path, 'w') as f:
        ts_old, value = f.get_timestamp('10', True)
        ts_new = booklet.utils.make_timestamp_int()
        f.set_timestamp('10', ts_new)

    with ebooklet.open(remote_conn, file_path) as f:
        ts_new = f.get_timestamp('10')

    assert ts_new > ts_old and value == data_dict['10']


def test_keys():
    with ebooklet.open(remote_conn, file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict.keys()))

    assert source_keys == keys


def test_items():
    with ebooklet.open(remote_conn, file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


def test_timestamps():
    with ebooklet.open(remote_conn, file_path) as f:
        for key, ts, value in f.timestamps(True):
            source_value = data_dict[key]
            assert source_value == value

        ts_new = booklet.utils.make_timestamp_int()
        for key, ts in f.timestamps():
            assert ts_new > ts


def test_contains():
    with ebooklet.open(remote_conn, file_path) as f:
        for key in data_dict:
            if key not in f:
                raise KeyError(key)

    assert True


def test_len():
    with ebooklet.open(remote_conn, file_path) as f:
        new_len = len(f)

    assert len(data_dict) == new_len


def test_delete_len():
    indexes = ['11', '12']

    for index in indexes:
        _ = data.pop(index)

        with ebooklet.open(remote_conn, file_path, 'w') as f:
            f[index] = 0
            f[index] = 0
            del f[index]

            f.sync()

            new_len = len(f)

            try:
                _ = f[index]
                raise ValueError()
            except KeyError:
                pass

        assert new_len == len(data)


def test_values():
    with ebooklet.open(remote_conn, file_path) as f:
        for value in f.values():
            pass

        for key, source_value in data.items():
            value = f[key]
            assert source_value == value


def test_prune():
    with ebooklet.open(remote_conn, file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f['2']

    assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, int)

    # Reindex
    # with ebooklet.open(remote_conn, file_path, 'w') as f:
    #     old_len = len(f)
    #     old_n_buckets = f._n_buckets
    #     removed_items = f.prune(reindex=True)
    #     new_n_buckets = f._n_buckets
    #     new_len = len(f)
    #     test_value = f['2']

    # assert (removed_items == 0) and (new_n_buckets > old_n_buckets) and (new_len == old_len) and isinstance(test_value, int)

    # Remove the rest via timestamp filter
    timestamp = booklet.utils.make_timestamp_int()

    with ebooklet.open(remote_conn, file_path, 'w') as f:
        removed_items = f.prune(timestamp=timestamp)
        new_len = len(f)
        meta = f.get_metadata()

    assert (old_len == removed_items) and (new_len == 0) and isinstance(meta, dict)


## Always make this last!!!
def test_clear():
    with ebooklet.open(remote_conn, file_path, 'w') as f:
        f.clear()
        f_meta = f.get_metadata()

        assert (len(f) == 0) and (len(list(f.keys())) == 0) and (f_meta is None)


#############################################################
### Sync with remotes


def test_push():
    with ebooklet.open(remote_conn, file_path, 'n', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

        f.set_metadata({'test': 'meta'})

        f.sync()

        changes = f.changes()
        print(list(changes.iter_changes())[0])
        changes.push()
        ri_path = f._remote_index_path

    ri_path.unlink()
    file_path.unlink()


def test_read_remote():
    http_remote = remote.S3Connection(db_url=db_url)

    with ebooklet.open(http_remote, file_path) as f:
        # value1 = f['10']
        # assert value1 == data_dict['10']

        counter = 0
        for key, value in f.get_items(f.keys()):
            source_value = data_dict[key]
            assert source_value == value
            counter += 1

        assert counter == len(data_dict)

        meta = f.get_metadata()
        assert len(meta) > 0

        ri_path = f._remote_index_path

    with ebooklet.open(http_remote, file_path) as f:
        pass

    ri_path.unlink()
    file_path.unlink()


############################################
### Test remote methods


def test_copy_remote():
    """

    """
    with remote_conn.open('w') as source_session:
        resp = source_session.copy_remote(remote_conn2)

    assert resp is None



############################################
### RemoteConnGroup

remote_conn_rcg = remote.S3Connection(access_key_id, access_key, db_key_rcg, bucket, endpoint_url=endpoint_url, db_url=db_url_rcg)

def test_remote_conn_grp_set():
    with remote_conn.open() as source_session:
        remote_conn_uuid_hex = source_session.uuid.hex

    with ebooklet.open(remote_conn_rcg, file_path_rcg, 'n', remote_conn_group=True) as f:
        f.add(remote_conn)

    with booklet.open(file_path_rcg) as f:
        conn_dict = f[remote_conn_uuid_hex]

        assert isinstance(conn_dict, dict)

def test_remote_conn_grp_push():
    with ebooklet.open(remote_conn_rcg, file_path_rcg, 'w') as f:
        changes = f.changes()
        # print(list(changes.iter_changes()))
        changes.push()
        ri_path = f._remote_index_path

    ri_path.unlink()
    file_path_rcg.unlink()

    assert True

def test_remote_conn_grp_read_remote():
    with remote_conn.open() as source_session:
        remote_conn_uuid_hex = source_session.uuid.hex
    # http_remote = remote.S3Connection(db_url=db_url)

    with ebooklet.open(db_url_rcg, file_path_rcg) as f:
        conn_dict = f[remote_conn_uuid_hex]
        ri_path = f._remote_index_path

    ri_path.unlink()
    file_path_rcg.unlink()

    assert isinstance(conn_dict, dict)


##################################
### Remove files

def test_remove_remote_local():
    with remote_conn.open('w') as s3open:
        s3open.delete_remote()

    with ebooklet.open(remote_conn2, file_path, 'n', value_serializer='pickle') as db:
       uuid1 = db._remote_session.get_uuid()
       assert uuid1 is None
    # with remote_conn2.open('w') as s3open:
    #     s3open.delete_remote()

    with remote_conn_rcg.open('w') as s3open:
        s3open.delete_remote()














































































