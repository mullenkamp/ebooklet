import pytest
import os, pathlib, io
import uuid6 as uuid
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
import ebooklet
from ebooklet import remote

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

bucket = 'achelous'
num_groups = 10

db_key_map = uuid.uuid8().hex[-13:]
file_path_map = script_path.joinpath(db_key_map)

db_key_map2 = uuid.uuid8().hex[-13:]
file_path_map2 = script_path.joinpath(db_key_map2)

remote_conn_map = remote.S3Connection(access_key_id, access_key, db_key_map, bucket, endpoint_url=endpoint_url)
remote_conn_map2 = remote.S3Connection(access_key_id, access_key, db_key_map2, bucket, endpoint_url=endpoint_url)

data_dict = {str(key): key * 2 for key in range(2, 20)}


#################################################
### Top-level picklable functions

def double_value(key, value):
    return (key, value * 2)


def skip_even(key, value):
    if value % 2 == 0:
        return None
    return (key, value)


def remap_key(key, value):
    return (f"new_{key}", value)


#################################################
### Fixtures

@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup test data after session."""
    def remove_test_data():
        for rc in [remote_conn_map, remote_conn_map2]:
            try:
                with rc.open('w') as s3open:
                    s3open.delete_remote()
            except:
                pass

        for fp in [file_path_map, file_path_map2]:
            ri = fp.parent.joinpath(fp.name + '.remote_index')
            for p in [fp, ri]:
                if p.exists():
                    p.unlink()

    request.addfinalizer(remove_test_data)


#################################################
### Tests


def test_map_all_keys():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value

        stats = f.map(double_value, n_workers=2)

    with ebooklet.open(remote_conn_map, file_path_map) as f:
        for key, value in data_dict.items():
            assert f[key] == value * 2


def test_map_specific_keys():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value

        subset = ['2', '5', '10']
        stats = f.map(double_value, keys=subset, n_workers=2)

    with ebooklet.open(remote_conn_map, file_path_map) as f:
        for key in subset:
            assert f[key] == data_dict[key] * 2

        # Unmapped keys should be unchanged
        assert f['15'] == data_dict['15']


def test_map_with_remote_data():
    # Write and push to S3
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value
        f.changes().push()
        ri_path = f._remote_index_path

    ri_path.unlink()
    file_path_map.unlink()

    # Reopen (data is on S3), map should load from remote first
    with ebooklet.open(remote_conn_map, file_path_map, 'w', value_serializer='pickle') as f:
        stats = f.map(double_value, n_workers=2)

        for key, value in data_dict.items():
            assert f[key] == value * 2


def test_map_separate_write_db():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as source:
        for key, value in data_dict.items():
            source[key] = value

        with ebooklet.open(remote_conn_map2, file_path_map2, 'n', value_serializer='pickle', num_groups=num_groups) as dest:
            stats = source.map(remap_key, write_db=dest, n_workers=2)

        with ebooklet.open(remote_conn_map2, file_path_map2) as dest:
            for key, value in data_dict.items():
                assert dest[f"new_{key}"] == value


def test_map_read_only_raises():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value

    with ebooklet.open(remote_conn_map, file_path_map) as f:
        with pytest.raises(ValueError, match='read only'):
            f.map(double_value, n_workers=2)


def test_map_read_only_with_write_db():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value

    with ebooklet.open(remote_conn_map, file_path_map) as source:
        with ebooklet.open(remote_conn_map2, file_path_map2, 'n', value_serializer='pickle', num_groups=num_groups) as dest:
            stats = source.map(double_value, write_db=dest, n_workers=2)

    with ebooklet.open(remote_conn_map2, file_path_map2) as dest:
        for key, value in data_dict.items():
            assert dest[key] == value * 2


def test_map_skip_none():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value

        stats = f.map(skip_even, n_workers=2)

    assert stats['written'] < stats['processed']


def test_map_stats():
    with ebooklet.open(remote_conn_map, file_path_map, 'n', value_serializer='pickle', num_groups=num_groups) as f:
        for key, value in data_dict.items():
            f[key] = value

        stats = f.map(double_value, n_workers=2)

    assert 'processed' in stats
    assert 'written' in stats
    assert 'errors' in stats
    assert stats['processed'] == len(data_dict)
    assert stats['written'] == len(data_dict)
    assert stats['errors'] == 0
