"""
Regression tests for the RemoteConnGroup 0.9.0 enhancement: keyed entries, explicit
user_meta, entry schema v1, write-time entry stamping, and upsert semantics.

Runs against the live test bucket, following test_push_integrity.py conventions.
"""
import io
import os
import pathlib
import tempfile

import pytest
import uuid6 as uuid

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

from ebooklet import open_ebooklet, open_rcg, remote

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

_conns = []
_paths = []


def make_conn(suffix):
    db_key = f'rcgent-{suffix}-' + uuid.uuid8().hex[-10:]
    conn = remote.S3Connection(access_key_id, access_key, db_key, bucket, endpoint_url=endpoint_url)
    _conns.append(conn)
    return conn


# local ebooklet cache files go to a temp dir, NOT the tests dir: a killed live-test run
# skips the session-scoped cleanup fixture, so writing here would leak DB files into the
# repo (regenerable but noisy). A temp dir keeps a killed run's leak in /tmp instead.
_local_tmp_dir = pathlib.Path(tempfile.mkdtemp(prefix='ebooklet-tests-'))


def local_path(name):
    p = _local_tmp_dir.joinpath(f'{name}-' + uuid.uuid8().hex[-10:])
    _paths.append(p)
    return p


def seed_member(suffix):
    """Create and push a tiny member remote for add() targets."""
    conn = make_conn(f'member-{suffix}')
    with open_ebooklet(conn, local_path(f'member-{suffix}'), flag='n') as eb:
        eb['seed'] = b'x'
        assert eb.changes().push()
    return conn


def member_uuid_hex(conn):
    with conn.open() as rc:
        return rc.uuid.hex


#################################################
### Pytest stuff


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    def remove_test_data():
        for conn in _conns:
            try:
                with conn.open('w') as s3open:
                    s3open.delete_remote()
            except Exception:
                pass
        for p in _paths:
            for f in p.parent.glob(p.name + '*'):
                f.unlink(missing_ok=True)

    request.addfinalizer(remove_test_data)


#################################################
### Tests


def test_add_with_key_and_user_meta_roundtrip():
    member = seed_member('m1')
    rcg_conn = make_conn('rcg1')
    meta = {'variable': 'air_temperature', 'owner': 'niwa', 'version': '2'}
    key = 'abc123def456abc123def456'

    with open_rcg(rcg_conn, local_path('w1'), flag='n') as rcg:
        rcg.add(member, key=key, user_meta=meta)
        assert rcg.changes().push()

    with open_rcg(rcg_conn, local_path('r1'), flag='r') as rcg:
        assert sorted(rcg.keys()) == [key]
        entry = rcg.get(key)
        assert entry['entry_version'] == 1
        assert entry['user_meta'] == meta
        assert 'remote_meta' in entry
        assert entry['remote_conn']['db_key'] == member.db_key
        assert isinstance(entry['timestamp'], int)
        assert 'access_key' not in entry['remote_conn']  # credentials never stored


def test_default_key_is_uuid_hex():
    member = seed_member('m2')
    rcg_conn = make_conn('rcg2')

    with open_rcg(rcg_conn, local_path('w2'), flag='n') as rcg:
        rcg.add(member)
        assert rcg.changes().push()

    with open_rcg(rcg_conn, local_path('r2'), flag='r') as rcg:
        assert list(rcg.keys()) == [member_uuid_hex(member)]


def test_metadata_only_upsert_pushes():
    ## Review finding 1 regression: an upsert changing only user_meta must still
    ## enter the changelog and push (pre-fix the entry carried the member's
    ## unchanged timestamp and was silently ignored).
    member = seed_member('m3')
    rcg_conn = make_conn('rcg3')

    with open_rcg(rcg_conn, local_path('w3'), flag='n') as rcg:
        rcg.add(member, key='k1', user_meta={'state': 'v1'})
        assert rcg.changes().push()

    with open_rcg(rcg_conn, local_path('w3b'), flag='w') as rcg:
        rcg.add(member, key='k1', user_meta={'state': 'v2'})
        assert rcg.changes().push()

    with open_rcg(rcg_conn, local_path('r3'), flag='r') as rcg:
        assert rcg.get('k1')['user_meta'] == {'state': 'v2'}


def test_explicit_key_upsert_cleans_default_entry():
    ## Review finding 5: migrating a default-keyed (uuid-hex) entry to an explicit
    ## key must not leave an orphaned duplicate.
    member = seed_member('m4')
    rcg_conn = make_conn('rcg4')

    with open_rcg(rcg_conn, local_path('w4'), flag='n') as rcg:
        rcg.add(member)
        assert rcg.changes().push()

    with open_rcg(rcg_conn, local_path('w4b'), flag='w') as rcg:
        rcg.add(member, key='dataset123')
        rcg.changes().push()

    with open_rcg(rcg_conn, local_path('r4'), flag='r') as rcg:
        assert sorted(rcg.keys()) == ['dataset123']


def test_setitem_and_delete_roundtrip():
    member = seed_member('m5')
    rcg_conn = make_conn('rcg5')

    with open_rcg(rcg_conn, local_path('w5'), flag='n') as rcg:
        rcg['entry-a'] = member
        assert rcg.changes().push()

    with open_rcg(rcg_conn, local_path('w5b'), flag='w') as rcg:
        assert rcg.get('entry-a')['user_meta'] is None
        del rcg['entry-a']
        rcg.changes().push()

    with open_rcg(rcg_conn, local_path('r5'), flag='r') as rcg:
        assert list(rcg.keys()) == []


def test_add_validation():
    member = seed_member('m6')
    rcg_conn = make_conn('rcg6')

    with open_rcg(rcg_conn, local_path('w6'), flag='n') as rcg:
        with pytest.raises(ValueError, match='object-key'):
            rcg.add(member, key='bad!key')
        with pytest.raises(TypeError, match='key must be a str'):
            rcg.add(member, key=123)
        with pytest.raises(TypeError, match='JSON-serializable'):
            rcg.add(member, key='okkey', user_meta={'x': object()})
