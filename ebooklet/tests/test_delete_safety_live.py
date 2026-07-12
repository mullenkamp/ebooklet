#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Live (B2) regression tests for the 0.9.5 exact-key delete fixes. The hermetic
tests in test_delete_safety.py prove ebooklet's call shape over a fake; these
prove the part only a real versioned store can: s3func's exact-key
version-resolution filter (s3func s3.py delete_objects keys+purge path), i.e.
that an exact-key purge on a real bucket removes ALL versions of exactly the
requested key and nothing else.

Live-gated via s3_config.toml / env vars like the other suites.
"""
import io
import os
import pathlib

import pytest
try:
    import tomllib as toml
except ImportError:
    import tomli as toml
import uuid6 as uuid

from ebooklet import open_ebooklet, remote, utils

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


def make_conn(db_key):
    conn = remote.S3Connection(access_key_id, access_key, db_key, bucket, endpoint_url=endpoint_url)
    _conns.append(conn)
    return conn


def local_path(name):
    p = script_path.joinpath(f'{name}-' + uuid.uuid8().hex[-10:])
    _paths.append(p)
    return p


def _key_for_group(gid, num_groups, taken=()):
    i = 0
    while True:
        k = f'k{i}'
        if utils.key_to_group_id(k, num_groups) == gid and k not in taken:
            return k
        i += 1


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    def remove_test_data():
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


def test_live_emptied_group_delete_spares_siblings():
    """Emptying group 1 (num_groups=13) must remove every version of exactly
    'db/1' and leave 'db/10' and 'db/12' fully intact on the real store."""
    num_groups = 13
    k1 = _key_for_group(1, num_groups)
    k10 = _key_for_group(10, num_groups)
    k12 = _key_for_group(12, num_groups)

    db_key = 'delsafe-grp-' + uuid.uuid8().hex[-10:]
    conn = make_conn(db_key)

    with open_ebooklet(conn, local_path('grp-w'), flag='n', num_groups=num_groups) as eb:
        eb[k1] = b'one'
        eb[k10] = b'ten'
        eb[k12] = b'twelve'
        assert eb.changes().push() is True

    with open_ebooklet(conn, local_path('grp-w2'), flag='w') as eb:
        del eb[k1]
        assert eb.changes().push() is True

    ## Real listing: exactly group 1 gone, siblings present.
    with conn.open('w') as s3open:
        child_keys = {o['key'] for o in s3open.list_objects().iter_objects()}
        assert f'{db_key}/1' not in child_keys
        assert f'{db_key}/10' in child_keys, 'exact-key delete removed sibling group 10'
        assert f'{db_key}/12' in child_keys, 'exact-key delete removed sibling group 12'
        ## Version-level proof: no version of 'db/1' survives (the purge), while
        ## sibling groups keep theirs.
        version_keys = [o['key'] for o in s3open._write_session.list_object_versions(prefix=f'{db_key}/1').iter_objects()]
        assert f'{db_key}/1' not in version_keys
        assert f'{db_key}/10' in version_keys

    with open_ebooklet(conn, local_path('grp-r'), flag='r') as eb:
        assert eb[k10] == b'ten'
        assert eb[k12] == b'twelve'
        assert k1 not in eb


def test_live_delete_remote_spares_sibling_db_and_own_lock():
    """delete_remote on 'X' must not touch sibling database 'X2', and must not
    delete the calling session's own live lock tickets (the flag='n' push runs
    it while holding that lock)."""
    db_key = 'delsafe-sib-' + uuid.uuid8().hex[-10:]
    sib_key = db_key + '2'
    conn = make_conn(db_key)
    sib_conn = make_conn(sib_key)

    ## Seed the sibling database first.
    with open_ebooklet(sib_conn, local_path('sib-w'), flag='n', num_groups=5) as eb:
        eb['other'] = b'precious'
        assert eb.changes().push() is True

    ## Seed the target database.
    with open_ebooklet(conn, local_path('tgt-w'), flag='n', num_groups=5) as eb:
        eb['mine'] = b'value'
        assert eb.changes().push() is True

    ## A write session holds its lock for its whole lifetime - delete the remote
    ## UNDER it and check the tickets survive (0.9.4's bare-prefix delete
    ## removed them, silently dropping mutual exclusion mid-'n'-push).
    eb = open_ebooklet(conn, local_path('tgt-w2'), flag='w')
    try:
        sess = eb._remote_session._write_session
        lock_prefix = db_key + '.lock.'
        tickets_before = [o['key'] for o in sess.list_objects(prefix=lock_prefix).iter_objects()]
        assert tickets_before, 'expected the write session to hold live lock tickets'

        eb.delete_remote()

        tickets_after = [o['key'] for o in sess.list_objects(prefix=lock_prefix).iter_objects()]
        assert tickets_after == tickets_before, 'delete_remote deleted the live lock tickets'

        ## The target db itself is gone...
        assert not [o for o in sess.list_objects(prefix=db_key + '/').iter_objects()]
    finally:
        eb.close()

    ## ...and the sibling database is untouched and fully readable.
    with open_ebooklet(sib_conn, local_path('sib-r'), flag='r') as eb2:
        assert eb2['other'] == b'precious'
