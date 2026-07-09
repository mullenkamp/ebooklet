#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Regression tests for the 0.9.3 "re-check then loud" 404-integrity protocol.

Pre-0.9.3, a 404 for an object the remote index claims was silently treated as
absence: keys vanished from map()/point reads far from the cause. Now the index
is re-pulled once per operation; keys the fresh index no longer claims resolve
to clean absence (stale local values purged), keys it still claims raise
RemoteIntegrityError.

Mock strategy: the key-selective mock 404s VALUE fetches only (get_object with a
non-None key); the re-check's db-object GET (get_object(None) via
fetch_remote_index) and metadata HEAD run for real, so the re-pull is genuine.
Live-gated via s3_config.toml / env vars like the other suites.
"""
import io
import logging
import os
import pathlib
import threading

import pytest
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
import uuid6 as uuid
import urllib3

from ebooklet import open_ebooklet, remote, RemoteIntegrityError
from ebooklet.utils import key_to_group_id, MissingRemoteObject, metadata_key_str

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
num_groups = 5

_conns = []
_paths = []


def make_conn(suffix):
    db_key = f'integ404-{suffix}-' + uuid.uuid8().hex[-10:]
    conn = remote.S3Connection(access_key_id, access_key, db_key, bucket, endpoint_url=endpoint_url)
    _conns.append(conn)
    return conn


def local_path(name):
    p = script_path.joinpath(f'{name}-' + uuid.uuid8().hex[-10:])
    _paths.append(p)
    return p


def seed_remote(conn, keys_values, ng=num_groups):
    p = local_path('seed404')
    kwargs = {'num_groups': ng} if ng is not None else {}
    with open_ebooklet(conn, p, flag='n', **kwargs) as eb:
        for k, v in keys_values.items():
            eb[k] = v
        assert eb.changes().push() is True


def sole_member_key(ng=num_groups):
    """A key that is the only member of its group when seeded alone."""
    return 'key000'


class Mock404Resp:
    status = 404
    error = {'Code': 'NoSuchKey', 'Message': 'injected 404'}
    data = b''
    metadata = {}


def patch_value_fetches_404(eb):
    """Key-selective: 404 value fetches, pass db-object fetches through."""
    original_get = eb._remote_session.get_object

    def mock_get(key=None, *args, **kwargs):
        if key is None:
            return original_get(key, *args, **kwargs)
        return Mock404Resp()

    eb._remote_session.get_object = mock_get
    return original_get


#################################################
### Pytest stuff


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    def remove_test_data():
        ## Loud, with one retry AND verify-by-listing (see test_push_integrity).
        for conn in _conns:
            for attempt in (1, 2):
                try:
                    with conn.open('w') as s3open:
                        s3open.delete_remote()
                        remaining = list(s3open.list_objects().iter_objects())
                        gone = not remaining and s3open.get_uuid() is None
                    if gone:
                        break
                    if attempt == 2:
                        print(f'cleanup: {conn.db_key} not fully deleted ({len(remaining)} child objects remain)')
                except Exception as e:
                    if attempt == 2:
                        print(f'cleanup: delete_remote failed for {conn.db_key}: {type(e).__name__}: {e}')
        for p in _paths:
            for f in p.parent.glob(p.name + '*'):
                f.unlink(missing_ok=True)

    request.addfinalizer(remove_test_data)


#################################################
### Point reads


def test_point_read_integrity_raises():
    ## Value fetch 404s; the real re-pull is a no-op (remote unchanged) so the
    ## index still claims the key -> confirmed integrity failure.
    k = sole_member_key()
    conn = make_conn('point')
    seed_remote(conn, {k: b'v1'})

    with open_ebooklet(conn, local_path('point-reader'), flag='r') as eb:
        patch_value_fetches_404(eb)
        with pytest.raises(RemoteIntegrityError, match='integrity'):
            eb.get(k)
        ## the instance stays usable and the failure is repeatable, not sticky-corrupt
        with pytest.raises(RemoteIntegrityError):
            eb[k]


def test_point_read_legit_deletion_quiet(caplog):
    ## NO mock: a writer really deletes the key; its group becomes empty so the
    ## group OBJECT is really deleted (upload_group only deletes empty groups).
    ## The stale reader's fetch 404s for real; the re-pull finds a fresh index
    ## that no longer claims the key -> clean absence, no raise.
    k = sole_member_key()
    conn = make_conn('legit')
    seed_remote(conn, {k: b'v1'})

    reader = open_ebooklet(conn, local_path('legit-reader'), flag='r')
    try:
        ## reader now holds the OLD index (claims k)
        with open_ebooklet(conn, local_path('legit-writer'), flag='w') as w:
            del w[k]
            assert w.changes().push() is True

        with caplog.at_level(logging.INFO, logger='ebooklet.main'):
            assert reader.get(k) is None
        assert any('deleted remotely' in rec.message for rec in caplog.records)

        assert k not in reader
        with pytest.raises(KeyError):
            reader[k]
        ## repeat get stays quiet (fresh index adopted, local value purged)
        assert reader.get(k) is None
    finally:
        reader.close()


def test_point_read_legit_deletion_quiet_per_key_mode():
    ## Same flow in legacy per-key mode: the writer's push deletes the per-key
    ## object itself.
    k = 'pk_key'
    conn = make_conn('legitpk')
    seed_remote(conn, {k: b'v1'}, ng=None)

    reader = open_ebooklet(conn, local_path('legitpk-reader'), flag='r')
    try:
        with open_ebooklet(conn, local_path('legitpk-writer'), flag='w') as w:
            del w[k]
            assert w.changes().push() is True

        assert reader.get(k) is None
        assert k not in reader
    finally:
        reader.close()


#################################################
### Metadata


def test_metadata_integrity_raises():
    k = sole_member_key()
    conn = make_conn('metaint')
    p = local_path('metaint-seed')
    with open_ebooklet(conn, p, flag='n', num_groups=num_groups) as eb:
        eb[k] = b'v1'
        eb.set_metadata({'m': 1})
        assert eb.changes().push() is True

    with open_ebooklet(conn, local_path('metaint-reader'), flag='r') as eb:
        patch_value_fetches_404(eb)
        with pytest.raises(RemoteIntegrityError):
            eb.get_metadata()


def test_metadata_legit_deletion_branch_unsets_local():
    ## The metadata quiet-branch is defensive: no public flow deletes ONLY the
    ## metadata (it only vanishes with a whole-remote wipe, which changes the
    ## uuid and takes the ValueError path instead). Exercise the branch
    ## directly: a stale local metadata value + a marker whose claim the
    ## current index does NOT hold must be unset (set_metadata(None)), so
    ## get_metadata() returns None instead of the stale value.
    k = sole_member_key()
    conn = make_conn('metadel')
    seed_remote(conn, {k: b'v1'})   # no metadata -> index does not claim it

    reader = open_ebooklet(conn, local_path('metadel-reader'), flag='r')
    try:
        ## simulate a stale materialized value (local file is always writable)
        reader._local_file.set_metadata({'m': 1})
        assert reader._local_file.get_metadata() == {'m': 1}

        marker = MissingRemoteObject('_metadata', [metadata_key_str])
        resolved = reader._resolve_missing({metadata_key_str: marker})
        assert resolved[metadata_key_str] is None

        assert reader.get_metadata() is None
    finally:
        reader.close()


#################################################
### Bulk (load_items / items / map)


def test_load_items_integrity_raises():
    k = sole_member_key()
    conn = make_conn('bulk')
    seed_remote(conn, {k: b'v1', 'other_key': b'v2'})

    with open_ebooklet(conn, local_path('bulk-reader'), flag='r') as eb:
        patch_value_fetches_404(eb)

        failure_dict = eb.load_items()
        assert failure_dict
        assert all(isinstance(v, MissingRemoteObject) for v in failure_dict.values())

        with pytest.raises(RemoteIntegrityError):
            list(eb.items())
        with pytest.raises(RemoteIntegrityError):
            list(eb.map(lambda key, value: (key, value)))


#################################################
### Push side


def test_push_pull_404_is_loud_and_retryable():
    ## Mirror of test_deletes_retained_when_group_pull_fails with a 404 instead
    ## of a 500: the old code silently dropped the group's members (self-heal);
    ## now the push reports a partial failure, keeps the delete for retry, and
    ## the unpulled member's value survives.
    conn = make_conn('pushfail')
    ## two keys in the SAME group
    same, _control = [], None
    i = 0
    keys = []
    while len(keys) < 2:
        cand = f'key{i:03d}'
        if not keys or key_to_group_id(cand, num_groups) == key_to_group_id(keys[0], num_groups):
            keys.append(cand)
        i += 1
    k1, k2 = keys
    seed_remote(conn, {k1: b'v1', k2: b'v2'})

    with open_ebooklet(conn, local_path('pushfail-writer'), flag='w') as eb:
        del eb[k1]  # k2 unmaterialized -> push must pull the group to repack

        original_get = eb._remote_session.get_object

        def mock_get(key=None, *args, **kwargs):
            if key is None:
                return original_get(key, *args, **kwargs)
            return Mock404Resp()

        eb._remote_session.get_object = mock_get
        result = eb.changes().push()
        eb._remote_session.get_object = original_get

        assert isinstance(result, dict) and result
        assert any(isinstance(v, MissingRemoteObject) for v in result.values())
        assert k1 in eb._deletes                 # retry signal retained

        assert eb.changes().push() is True       # retry completes

    with open_ebooklet(conn, local_path('pushfail-reader'), flag='r') as eb:
        assert eb.get(k1) is None
        assert eb.get(k2) == b'v2'               # old silent path dropped k2


#################################################
### Concurrency + live race


def test_concurrent_readers_during_recheck():
    ## Threads point-read while one thread's read triggers the re-check (which
    ## swaps the index handle). Under _index_lock nobody may observe a closed
    ## index (pre-0.9.3 design would have crashed with closed-file ValueErrors).
    k = sole_member_key()
    conn = make_conn('conc')
    seed_remote(conn, {k: b'v1', 'other_key': b'v2'})

    reader = open_ebooklet(conn, local_path('conc-reader'), flag='r')
    try:
        with open_ebooklet(conn, local_path('conc-writer'), flag='w') as w:
            del w[k]
            assert w.changes().push() is True

        errors = []
        stop = threading.Event()

        def hammer():
            while not stop.is_set():
                try:
                    reader.get('other_key')
                except RemoteIntegrityError:
                    pass  # acceptable: transient claim during the window
                except Exception as e:  # noqa: BLE001
                    errors.append(e)

        threads = [threading.Thread(target=hammer, daemon=True) for _ in range(3)]
        for t in threads:
            t.start()
        try:
            ## triggers the real 404 -> re-check -> index swap under load
            assert reader.get(k) is None
        finally:
            stop.set()
            for t in threads:
                t.join(10)
        assert not errors, errors
    finally:
        reader.close()


def test_live_group_object_deleted_under_reader():
    ## True integrity fault, no mocks: the group object is deleted directly
    ## (index untouched), so the re-pull is a no-op and the fresh index still
    ## claims the key -> loud.
    k = sole_member_key()
    conn = make_conn('live')
    seed_remote(conn, {k: b'v1'})
    gid = key_to_group_id(k, num_groups)

    with open_ebooklet(conn, local_path('live-reader'), flag='r') as eb:
        with conn.open('w') as s:
            s.delete_object(str(gid))

        with pytest.raises(RemoteIntegrityError, match='integrity'):
            eb.get(k)
