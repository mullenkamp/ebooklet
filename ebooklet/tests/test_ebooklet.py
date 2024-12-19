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
import ebooklet
from ebooklet import __version__, EBooklet, remote
from copy import deepcopy

#################################################
### Parameters

script_path = pathlib.Path(os.path.realpath(os.path.dirname(__file__)))

try:
    with open(script_path.joinpath('s3_config.toml'), "rb") as f:
        conn_config = toml.load(f)['connection_config']
except:
    conn_config = {
        # 'service_name': 's3',
        'endpoint_url': os.environ['endpoint_url'],
        'aws_access_key_id': os.environ['aws_access_key_id'],
        'aws_secret_access_key': os.environ['aws_secret_access_key'],
        }

# tf = NamedTemporaryFile()
# file_path = tf.name

# connection_config = conn_config
bucket = 'achelous'
flag = "n"
buffer_size = 524288
read_timeout = 60
threads = 10
# db_key = 'test.blt'
db_key = uuid.uuid8().hex[:13]
file_path = script_path.joinpath(db_key)
base_url = 'https://b2.tethys-ts.xyz/file/' + bucket + '/'
db_url = base_url +  db_key
value_serializer = 'pickle'
remote_object_lock=False
init_remote=True
local_storage_kwargs = {}

data_dict = {str(key): key*2 for key in range(2, 30)}

data = deepcopy(data_dict)

meta = {'test1': 'data'}



################################################
### Tests

print(__version__)

s3_conn = remote.S3Conn(db_key, bucket, **conn_config)
http_conn = remote.HttpConn(db_url)

remote_conn = remote.Conn(s3_conn=s3_conn, http_conn=http_conn)


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

        s3open = s3_conn.open()
        s3open.delete_remote()

        file_path.unlink()
        remote_index_path = file_path.parent.joinpath(file_path.name + '.remote_index')
        remote_index_path.unlink()


    # elif request.node.rep_call.passed:
    #     # Add code here to cleanup success scenario
    #     print("executing test success")

################################################
### Normal local operations


def test_set_items():
    with EBooklet(remote_conn, file_path, 'n', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

    with EBooklet(http_conn, file_path) as f:
        value = f['10']

    assert value == data_dict['10']


def test_update():
    with EBooklet(remote_conn, file_path, 'n', value_serializer='pickle') as f:
        f.update(data_dict)

    with EBooklet(http_conn, file_path) as f:
        value = f['10']

    assert value == data_dict['10']


def test_set_get_metadata():
    """

    """
    with EBooklet(remote_conn, file_path, 'w') as f:
        old_meta = f.get_metadata()
        f.set_metadata(meta)

    assert old_meta is None

    with EBooklet(http_conn, file_path) as f:
        new_meta = f.get_metadata()

    assert new_meta == meta


def test_set_get_timestamp():
    with EBooklet(remote_conn, file_path, 'w') as f:
        ts_old, value = f.get_timestamp('10', True)
        ts_new = booklet.utils.make_timestamp_int()
        f.set_timestamp('10', ts_new)

    with EBooklet(http_conn, file_path) as f:
        ts_new = f.get_timestamp('10')

    assert ts_new > ts_old and value == data_dict['10']


def test_keys():
    with EBooklet(http_conn, file_path) as f:
        keys = set(list(f.keys()))

    source_keys = set(list(data_dict.keys()))

    assert source_keys == keys


def test_items():
    with EBooklet(http_conn, file_path) as f:
        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


def test_timestamps():
    with EBooklet(http_conn, file_path) as f:
        for key, ts, value in f.timestamps(True):
            source_value = data_dict[key]
            assert source_value == value

        ts_new = booklet.utils.make_timestamp_int()
        for key, ts in f.timestamps():
            assert ts_new > ts


def test_contains():
    with EBooklet(http_conn, file_path) as f:
        for key in data_dict:
            if key not in f:
                raise KeyError(key)

    assert True


def test_len():
    with EBooklet(http_conn, file_path) as f:
        new_len = len(f)

    assert len(data_dict) == new_len


def test_delete_len():
    indexes = ['11', '12']

    for index in indexes:
        _ = data.pop(index)

        with EBooklet(s3_conn, file_path, 'w') as f:
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


# def test_items2():
#     with EBooklet(s3_conn, file_path, init_check_remote=False) as f:
#         for key, value in f.items():
#             source_value = data[key]
#             assert source_value == value


def test_values():
    with EBooklet(s3_conn, file_path) as f:
        for value in f.values():
            pass

        for key, source_value in data.items():
            value = f[key]
            assert source_value == value


def test_prune():
    with EBooklet(s3_conn, file_path, 'w') as f:
        old_len = len(f)
        removed_items = f.prune()
        new_len = len(f)
        test_value = f['2']

    assert (removed_items > 0)  and (old_len > removed_items) and (new_len == old_len) and isinstance(test_value, int)

    # Reindex
    with EBooklet(s3_conn, file_path, 'w') as f:
        old_len = len(f)
        old_n_buckets = f._n_buckets
        removed_items = f.prune(reindex=True)
        new_n_buckets = f._n_buckets
        new_len = len(f)
        test_value = f['2']

    assert (removed_items == 0) and (new_n_buckets > old_n_buckets) and (new_len == old_len) and isinstance(test_value, int)

    # Remove the rest via timestamp filter
    timestamp = booklet.utils.make_timestamp_int()

    with EBooklet(s3_conn, file_path, 'w') as f:
        removed_items = f.prune(timestamp=timestamp)
        new_len = len(f)
        meta = f.get_metadata()

    assert (old_len == removed_items) and (new_len == 0) and isinstance(meta, dict)


# def test_set_items_get_items():
#     with EBooklet(remote, file_path, 'n', key_serializer='uint4', value_serializer='pickle') as f:
#         for key, value in data_dict.items():
#             f[key] = value

#     with EBooklet(remote, file_path, 'w') as f:
#         f[50] = [0, 0]
#         value1 = f[10]
#         value2 = f[50]

#     assert (value1 == data_dict[10]) and (value2 == [0, 0])

    # with EBooklet(file_path) as f:
    #     value = f[50]
    #     assert value == [0, 0]

    #     value = f[10]
    #     assert value == data_dict[10]

## Always make this last!!!
def test_clear():
    with EBooklet(s3_conn, file_path, 'w') as f:
        f.clear()
        f_meta = f.get_metadata()

        assert (len(f) == 0) and (len(list(f.keys())) == 0) and (f_meta is None)


#############################################################
### Sync with remotes


def test_push():
    with EBooklet(remote_conn, file_path, 'n', value_serializer='pickle') as f:
        for key, value in data_dict.items():
            f[key] = value

        f.sync()

        changes = f.changes()
        print(list(changes.iter_changes()))
        changes.push()
        ri_path = f._remote_index_path

    ri_path.unlink()
    file_path.unlink()


def test_read_remote():
    http_remote = remote.HttpConn(db_url)

    with EBooklet(http_remote, file_path) as f:
        value1 = f['10']
        assert value1 == data_dict['10']

        for key, value in f.items():
            source_value = data_dict[key]
            assert source_value == value


##################################
### Remove files

def test_remove_remote_local():
    s3open = s3_conn.open()
    s3open.delete_remote()

    file_path.unlink()
    remote_index_path = file_path.parent.joinpath(file_path.name + '.remote_index')
    remote_index_path.unlink()














































































