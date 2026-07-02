"""
Regression tests for the two push-integrity fixes (v0.8.4):

1. Grouped-mode pushes must repack the FULL membership of each affected group —
   members not materialized in the writer's local file must be pulled down first,
   never silently dropped (which corrupted group-mates: wrong-value reads,
   InvalidRange errors, dangling index entries).
2. Remote session metadata (uuid/timestamp/init_bytes/num_groups) must be re-read
   after the write lock is acquired, so a writer whose session was created before
   the remote existed does not skip the index pull and clobber another writer
   (bootstrap race). Relatedly, pushing a brand-new EMPTY database must still
   materialize the remote db object.

These tests run against the live test bucket, following test_ebooklet.py conventions.
"""
import io
import os
import pathlib

import pytest
import uuid6 as uuid

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

from ebooklet import open_ebooklet, open_rcg, remote, RemoteConnGroup
from ebooklet.utils import key_to_group_id, pack_group

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
num_groups = 5  # prime, so no rounding; small enough to force group collisions

_conns = []
_paths = []


def make_conn(suffix):
    db_key = f'integrity-{suffix}-' + uuid.uuid8().hex[-10:]
    conn = remote.S3Connection(access_key_id, access_key, db_key, bucket, endpoint_url=endpoint_url)
    _conns.append(conn)
    return conn


def local_path(name):
    p = script_path.joinpath(f'{name}-' + uuid.uuid8().hex[-10:])
    _paths.append(p)
    return p


def colliding_keys(n_same=3):
    """Return n_same keys hashing to one group plus one control key in another group."""
    same, control = [], None
    i = 0
    while len(same) < n_same or control is None:
        k = f'key{i:03d}'
        gid = key_to_group_id(k, num_groups)
        if len(same) < n_same and (not same or gid == key_to_group_id(same[0], num_groups)):
            same.append(k)
        elif control is None and same and gid != key_to_group_id(same[0], num_groups):
            control = k
        i += 1
    return same, control


def seed_remote(conn, keys_values):
    p = local_path('seed')
    with open_ebooklet(conn, p, flag='n', num_groups=num_groups) as eb:
        for k, v in keys_values.items():
            eb[k] = v
        assert eb.changes().push() is True


def read_all(conn, keys):
    p = local_path('reader')
    out = {}
    with open_ebooklet(conn, p, flag='r') as eb:
        stored_keys = sorted(k for k in eb.keys())
        for k in keys:
            out[k] = eb.get(k)
    return out, stored_keys


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
### Fix 1: grouped pushes repack full group membership


def test_grouped_push_preserves_unmaterialized_members():
    (k1, k2, k3), c1 = colliding_keys()
    conn = make_conn('clobber')
    seed_remote(conn, {k1: b'value-K1', k2: b'value-K2', c1: b'value-C1'})

    ## Fresh local file: only k3 is materialized locally; its group-mates k1/k2 must
    ## be pulled and repacked, not dropped.
    with open_ebooklet(conn, local_path('writer2'), flag='w') as eb:
        eb[k3] = b'value-K3'
        assert eb.changes().push() is True

    values, stored_keys = read_all(conn, [k1, k2, k3, c1])
    assert values == {k1: b'value-K1', k2: b'value-K2', k3: b'value-K3', c1: b'value-C1'}
    assert stored_keys == sorted([k1, k2, k3, c1])


def test_grouped_delete_from_fresh_copy():
    (k1, k2, k3), c1 = colliding_keys()
    conn = make_conn('delete-one')
    seed_remote(conn, {k1: b'value-K1', k2: b'value-K2', k3: b'value-K3', c1: b'value-C1'})

    with open_ebooklet(conn, local_path('deleter'), flag='w') as eb:
        del eb[k1]
        eb.changes().push()

    values, stored_keys = read_all(conn, [k1, k2, k3, c1])
    assert values == {k1: None, k2: b'value-K2', k3: b'value-K3', c1: b'value-C1'}
    assert stored_keys == sorted([k2, k3, c1])


def test_grouped_delete_all_members():
    (k1, k2, _), c1 = colliding_keys()
    conn = make_conn('delete-all')
    seed_remote(conn, {k1: b'value-K1', k2: b'value-K2', c1: b'value-C1'})

    with open_ebooklet(conn, local_path('deleter2'), flag='w') as eb:
        del eb[k1]
        del eb[k2]
        eb.changes().push()

    values, stored_keys = read_all(conn, [k1, k2, c1])
    assert values == {k1: None, k2: None, c1: b'value-C1'}
    assert stored_keys == [c1]

    ## The emptied group's object must be gone from the remote.
    gid = key_to_group_id(k1, num_groups)
    with conn.open('w') as s:
        object_keys = [o['key'] for o in s.list_objects().iter_objects()]
    assert f'{conn.db_key}/{gid}' not in object_keys


#################################################
### Fix 2: session metadata refreshed post-lock; empty pushes materialize


def test_bootstrap_race_second_writer_preserves_first():
    ## Writer B's remote SESSION is created while the remote does NOT yet exist, so
    ## its cached metadata says uuid=None. Writer A then creates and pushes the
    ## remote. When B opens (acquiring the lock uncontended), the post-lock metadata
    ## refresh must discover the now-existing remote and pull its index — instead of
    ## trusting the stale uuid=None and clobbering A's push (the pre-0.8.4 behavior).
    ##
    ## Deliberately deterministic (no threads): a truly concurrent variant also races
    ## the s3func bakery lock's LIST-after-PUT visibility window, an upstream s3func
    ## weakness that makes such a test flaky independent of this repo's fix.
    seed_a = make_conn('race-seed-a')
    seed_b = make_conn('race-seed-b')
    seed_remote(seed_a, {'seed': b'a'})
    seed_remote(seed_b, {'seed': b'b'})

    rcg_conn = make_conn('race-rcg')

    ## B's session: created before the remote exists -> stale uuid=None
    stale_session = rcg_conn.open('w')

    ## A creates the remote and pushes its entry
    with open_rcg(rcg_conn, local_path('race-a'), flag='n') as a:
        a.add(seed_a)
        assert a.changes().push() is True

    ## Precondition for the regression: B's cached view predates A's push
    assert stale_session.uuid is None

    b = RemoteConnGroup(stale_session, local_path('race-b'), flag='w')
    b.add(seed_b)
    b.changes().push()
    b.close()

    with open_rcg(rcg_conn, local_path('race-reader'), flag='r') as rcg:
        entry_db_keys = sorted(v['remote_conn']['db_key'] for v in rcg.values())
    assert entry_db_keys == sorted([seed_a.db_key, seed_b.db_key])


def test_empty_push_materializes_remote():
    conn = make_conn('empty-eb')
    with open_ebooklet(conn, local_path('empty-writer'), flag='n', num_groups=num_groups) as eb:
        assert eb.changes().push() is True

    ## Pre-fix this raised: 'No file was found in the remote...'
    with open_ebooklet(conn, local_path('empty-reader'), flag='r') as eb:
        assert list(eb.keys()) == []


def test_empty_push_materializes_remote_rcg():
    conn = make_conn('empty-rcg')
    with open_rcg(conn, local_path('empty-rcg-writer'), flag='n') as rcg:
        assert rcg.changes().push() is True

    with open_rcg(conn, local_path('empty-rcg-reader'), flag='r') as rcg:
        assert list(rcg.keys()) == []


#################################################
### Review-round fixes (Gemini finding 1)


def test_corrupted_group_object_recovery():
    ## Simulates a remote corrupted by a pre-0.8.4 partial push: the group object
    ## is rewritten keeping only k2 (relocated), so index offsets for k1/k2 are
    ## stale and k1's bytes are truly gone. A fresh-copy push of k3 must NOT
    ## launder wrong bytes: k2 recovered via the full-object fallback, k1 reported
    ## lost and dropped from the index, k3 written.
    (k1, k2, k3), c1 = colliding_keys()
    conn = make_conn('corrupt')
    seed_remote(conn, {k1: b'value-K1', k2: b'value-K2', c1: b'value-C1'})
    gid = key_to_group_id(k1, num_groups)

    with open_ebooklet(conn, local_path('corrupt-ts'), flag='r') as eb:
        k2_ts = eb.get_timestamp(k2)

    packed, _ = pack_group([(k2, k2_ts, b'value-K2')])
    with conn.open('w') as s:
        resp = s.put_object(str(gid), packed)
        assert resp.status // 100 == 2

    with open_ebooklet(conn, local_path('corrupt-writer'), flag='w') as eb:
        eb[k3] = b'value-K3'
        eb.changes().push()

    values, stored_keys = read_all(conn, [k1, k2, k3, c1])
    assert values[k2] == b'value-K2'   # recovered, NOT laundered garbage
    assert values[k3] == b'value-K3'
    assert values[c1] == b'value-C1'
    assert values[k1] is None          # truly lost -> dropped, loudly
    assert stored_keys == sorted([k2, k3, c1])


def test_deletes_retained_when_group_pull_fails():
    ## Fault-injection regression for the deletes-retention fix (adapted from a
    ## Gemini review-round experiment): if repacking a group fails because its
    ## unmaterialized members cannot be pulled, the push must (a) report the
    ## failure, (b) leave the remote group object untouched, and (c) KEEP the
    ## group's deletes in _deletes so a retry push repacks the group.
    (k1, k2, _), _ = colliding_keys()
    conn = make_conn('delete-fail')
    seed_remote(conn, {k1: b'v1', k2: b'v2'})

    with open_ebooklet(conn, local_path('deleter'), flag='w') as eb:
        del eb[k1]  # k2 is not materialized locally -> push must pull it to repack

        original_get = eb._remote_session.get_object

        def failing_get(*args, **kwargs):
            class MockResp:
                status = 500
                error = 'injected pull failure'
            return MockResp()

        eb._remote_session.get_object = failing_get
        result = eb.changes().push()
        eb._remote_session.get_object = original_get

        assert isinstance(result, dict) and result   # push reported the failure
        assert k1 in eb._deletes                     # retry signal retained

        assert eb.changes().push() is True           # retry completes the delete

    values, stored_keys = read_all(conn, [k1, k2])
    assert values == {k1: None, k2: b'v2'}
    assert stored_keys == [k2]
