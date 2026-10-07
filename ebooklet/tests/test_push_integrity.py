"""
Regression tests for the two push-integrity fixes (v0.8.4):

1. Grouped-mode pushes must repack the FULL membership of each affected group —
   members not materialized in the writer's local file must be pulled down first,
   never silently dropped (which corrupted group-mates: wrong-value reads,
   InvalidRange errors, dangling index entries).
2. Remote session metadata (uuid/timestamp/init_bytes/format) must be re-read
   after the write lock is acquired, so a writer whose session was created before
   the remote existed does not skip the index pull and clobber another writer
   (bootstrap race). Relatedly, pushing a brand-new EMPTY database must still
   materialize the remote db object.

These tests run against the live test bucket, following test_ebooklet.py conventions.
"""
import datetime
import io
import os
import pathlib
import shutil
import tempfile
import warnings

import pytest
import uuid6 as uuid

try:
    import tomllib as toml
except ImportError:
    import tomli as toml

from ebooklet import open_ebooklet, open_rcg, remote, RemoteConnGroup
from ebooklet.utils import pack_group
from ebooklet.tests.groups import gid_of

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
## The seed packing target. With colliding_keys' names and the tests' ~8-byte
## values a 'key00N' entry packs to 27 bytes and the long control key's to 58:
## seeded in the order c1, k1, k2 the control fills group 0 alone (4+58+27 > 85)
## and k1, k2 share the TAIL group, which a later k3 tops up exactly (4+3*27 = 85).
## Tests assert the layout they rely on (gids) - placement is allocation, not hashing.
group_bytes = 85

_conns = []
_paths = []


def make_conn(suffix):
    db_key = f'integrity-{suffix}-' + uuid.uuid8().hex[-10:]
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


def colliding_keys(n_same=3):
    """n_same short keys plus one long control key. Seeded control-first at
    group_bytes, the short keys share the tail group and the control sits in an
    earlier one (see group_bytes); each test asserts the layout via remote_gids."""
    return [f'key{i:03d}' for i in range(n_same)], 'ctl-' + 'x' * 33


def remote_gids(conn):
    """{key: gid} from a fresh reader of the committed index."""
    with open_ebooklet(conn, local_path('gids'), flag='r') as eb:
        return {k: gid_of(eb, k) for k in eb.keys()}


def seed_remote(conn, keys_values):
    p = local_path('seed')
    with open_ebooklet(conn, p, flag='n', group_bytes=group_bytes) as eb:
        for k, v in keys_values.items():
            eb[k] = v
        assert eb.changes().push()


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
        ## Loud, with one retry AND verify-by-listing. Deleting can fail without
        ## raising (s3func delete_objects discards the multi-delete POST response),
        ## and a killed run (e.g. 2026-07-03: a suite SIGTERM'd by a harness timeout
        ## leaked 17 remotes) never reaches teardown at all - so only an
        ## after-the-fact listing actually proves the remote is gone.
        for conn in _conns:
            for attempt in (1, 2):
                try:
                    with conn.open('w') as s3open:
                        s3open.delete_remote()
                        ## list covers db_key/* children; get_uuid covers the bare index object
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
### Fix 1: grouped pushes repack full group membership


def test_grouped_push_preserves_unmaterialized_members():
    (k1, k2, k3), c1 = colliding_keys()
    conn = make_conn('clobber')
    seed_remote(conn, {c1: b'value-C1', k1: b'value-K1', k2: b'value-K2'})
    g = remote_gids(conn)
    assert g[k1] == g[k2] != g[c1] and g[k1] == max(g.values()), 'precondition: k1/k2 are the tail'

    ## Fresh local file: only k3 is materialized locally; its group-mates k1/k2 must
    ## be pulled and repacked, not dropped.
    with open_ebooklet(conn, local_path('writer2'), flag='w') as eb:
        eb[k3] = b'value-K3'
        assert eb.changes().push()

    values, stored_keys = read_all(conn, [k1, k2, k3, c1])
    assert values == {k1: b'value-K1', k2: b'value-K2', k3: b'value-K3', c1: b'value-C1'}
    assert stored_keys == sorted([k1, k2, k3, c1])
    assert remote_gids(conn)[k3] == g[k1], 'k3 did not top up the tail group'


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
    seed_remote(conn, {c1: b'value-C1', k1: b'value-K1', k2: b'value-K2'})
    g = remote_gids(conn)
    assert g[k1] == g[k2] != g[c1], 'precondition'
    gid = g[k1]

    with open_ebooklet(conn, local_path('deleter2'), flag='w') as eb:
        del eb[k1]
        del eb[k2]
        eb.changes().push()

    values, stored_keys = read_all(conn, [k1, k2, c1])
    assert values == {k1: None, k2: None, c1: b'value-C1'}
    assert stored_keys == [c1]

    ## The emptied group's object must be gone from the remote.
    with conn.open('w') as s:
        object_keys = [o['key'] for o in s.list_objects().iter_objects()]
    assert not any(k.startswith(f'{conn.db_key}/{gid}.') for k in object_keys)


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
        assert a.changes().push()

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
    with open_ebooklet(conn, local_path('empty-writer'), flag='n', group_bytes=group_bytes) as eb:
        assert eb.changes().push()

    ## Pre-fix this raised: 'No file was found in the remote...'
    with open_ebooklet(conn, local_path('empty-reader'), flag='r') as eb:
        assert list(eb.keys()) == []


def test_empty_push_materializes_remote_rcg():
    conn = make_conn('empty-rcg')
    with open_rcg(conn, local_path('empty-rcg-writer'), flag='n') as rcg:
        assert rcg.changes().push()

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
    seed_remote(conn, {c1: b'value-C1', k1: b'value-K1', k2: b'value-K2'})
    g = remote_gids(conn)
    assert g[k1] == g[k2] != g[c1] and g[k1] == max(g.values()), 'precondition: k1/k2 are the tail'
    gid = g[k1]

    with open_ebooklet(conn, local_path('corrupt-ts'), flag='r') as eb:
        k2_ts = eb.get_timestamp(k2)

    packed, _ = pack_group([(k2, k2_ts, b'value-K2')])
    with conn.open('w') as s:
        ## Corrupt the group's LIVE generation IN PLACE (format 2 pushes never
        ## do this - that is the point of the damage primitive).
        children = [o['key'] for o in s.list_objects().iter_objects()]
        live = [k for k in children if k.startswith(f'{conn.db_key}/{gid}.')]
        assert live, f'no generation object found for group {gid}'
        resp = s.put_object(live[0].removeprefix(conn.db_key + '/'), packed)
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
    ## Fault-injection regression for the retention fix (adapted from a Gemini
    ## review-round experiment): if repacking a group fails because its
    ## unmaterialized members cannot be pulled, the push must (a) report the
    ## failure, (b) leave the remote group object untouched, and (c) KEEP the
    ## pending change journaled so a retry push repacks the group. (0.11: the
    ## change is an UPDATE - deletes are lazy and repack nothing.)
    (k1, k2, _), _ = colliding_keys()
    conn = make_conn('delete-fail')
    seed_remote(conn, {k1: b'v1', k2: b'v2'})
    g = remote_gids(conn)
    assert g[k1] == g[k2], 'precondition: one group'

    with open_ebooklet(conn, local_path('deleter'), flag='w') as eb:
        eb[k1] = b'v1-new'  # k2 is not materialized locally -> push must pull it to repack

        original_get = eb._remote_session.get_object

        def failing_get(*args, **kwargs):
            class MockResp:
                status = 500
                error = 'injected pull failure'
            return MockResp()

        eb._remote_session.get_object = failing_get
        result = eb.changes().push()
        eb._remote_session.get_object = original_get

        assert result.failures                       # push reported the failure
        assert k1 in eb._journal.written             # retry signal retained

        assert eb.changes().push()           # retry completes the update

    values, stored_keys = read_all(conn, [k1, k2])
    assert values == {k1: b'v1-new', k2: b'v2'}
    assert stored_keys == [k1, k2]


#################################################
### 0.9.0 items: 'n' guard + writes-only, pull() repair, prune contract


def test_flag_n_storage_mode_guard_and_lock_release():
    conn = make_conn('nguard')
    seed_remote(conn, {'k': b'v'})  # grouped remote

    with pytest.raises(ValueError, match='storage mode'):
        open_ebooklet(conn, local_path('nguard-a'), flag='n', group_bytes=None)

    ## The rejected open must have released the lock: an immediate open succeeds.
    with open_ebooklet(conn, local_path('nguard-b'), flag='n', group_bytes=group_bytes, lock_timeout=15) as eb:
        assert eb.group_bytes == group_bytes


def test_lock_released_on_uuid_mismatch():
    ## Review finding 4 regression: pre-existing early-raise paths in _init_common
    ## (here: local/remote UUID mismatch) must release the lock immediately, not at GC.
    conn_a = make_conn('mismatch-a')
    conn_b = make_conn('mismatch-b')
    seed_remote(conn_a, {'k': b'v'})
    seed_remote(conn_b, {'k': b'v'})

    pa = local_path('mismatch-local')
    with open_ebooklet(conn_a, pa, flag='w'):
        pass

    with pytest.raises(ValueError, match='different UUID'):
        open_ebooklet(conn_b, pa, flag='w')

    with open_ebooklet(conn_b, local_path('mismatch-ok'), flag='w', lock_timeout=15) as eb:
        assert eb.writable


def test_flag_n_writes_only_push():
    ## 'n' contract (Mike's decision): old remote stays readable pre-push, but the
    ## pushed new database contains ONLY this session's writes - reads never
    ## resurrect old keys, and the pushed index has no dangling entries.
    conn = make_conn('nwrites')
    seed_remote(conn, {'old1': b'old-1', 'old2': b'old-2'})

    with open_ebooklet(conn, local_path('nwrites-w'), flag='n', group_bytes=group_bytes) as eb:
        assert eb.get('old1') == b'old-1'  # transparent read still works pre-push
        eb['new1'] = b'new-1'
        assert eb.changes().push()

    with open_ebooklet(conn, local_path('nwrites-r'), flag='r') as eb:
        assert sorted(eb.keys()) == ['new1']
        assert eb.get('new1') == b'new-1'
        assert eb.get('old1') is None


def test_pull_stranded_reader():
    ## A reader session opened (on a copied local file) BEFORE the remote exists
    ## caches uuid=None; pull() must reload full metadata and refresh the index.
    conn = make_conn('stranded')
    producer_path = local_path('stranded-producer')
    with open_ebooklet(conn, producer_path, flag='n', group_bytes=group_bytes) as eb:
        eb['k1'] = b'v1'

    reader_path = local_path('stranded-reader')
    shutil.copy(producer_path, reader_path)

    reader = open_ebooklet(conn, reader_path, flag='r')
    assert reader._remote_session.uuid is None

    with open_ebooklet(conn, producer_path, flag='w', group_bytes=group_bytes) as eb:
        eb['k2'] = b'v2'
        assert eb.changes().push()

    reader.changes().pull()
    assert sorted(reader.keys()) == ['k1', 'k2']
    assert reader.get('k2') == b'v2'
    reader.close()


def test_pull_sees_remote_advance():
    conn = make_conn('pull-adv')
    seed_remote(conn, {'k1': b'v1'})

    reader = open_ebooklet(conn, local_path('pull-adv-r'), flag='r')
    assert reader.get('k1') == b'v1'

    with open_ebooklet(conn, local_path('pull-adv-w'), flag='w') as eb:
        eb['k2'] = b'v2'
        assert eb.changes().push()

    reader.changes().pull()
    assert reader.get('k2') == b'v2'
    reader.close()


def test_pull_fetch_failure_leaves_session_usable():
    ## Review finding 3 regression: a failed index download during pull() must
    ## leave the live session fully usable (fetch-first ordering).
    conn = make_conn('pull-fail')
    seed_remote(conn, {'k1': b'v1'})

    reader = open_ebooklet(conn, local_path('pull-fail-r'), flag='r')
    assert reader.get('k1') == b'v1'

    with open_ebooklet(conn, local_path('pull-fail-w'), flag='w') as eb:
        eb['k2'] = b'v2'
        assert eb.changes().push()

    original_get = reader._remote_session.get_object

    def failing_get(*args, **kwargs):
        class MockResp:
            status = 500
            error = 'injected fetch failure'
        return MockResp()

    reader._remote_session.get_object = failing_get
    with pytest.raises(Exception):
        reader.changes().pull()
    reader._remote_session.get_object = original_get

    assert reader.get('k1') == b'v1'   # session untouched by the failed pull
    reader.changes().pull()
    assert reader.get('k2') == b'v2'
    reader.close()


def test_prune_local_eviction_contract():
    ## Decided contract: prune reclaims local space only; the remote is untouched;
    ## evicted values re-pull on demand; pushes repack from the remote's membership.
    conn = make_conn('prune')
    keys = {f'pk{i}': f'val-{i}'.encode() for i in range(4)}
    seed_remote(conn, keys)

    with open_ebooklet(conn, local_path('prune-w'), flag='w') as eb:
        for k, v in keys.items():  # materialize everything locally
            assert eb.get(k) == v
        removed = eb.prune(timestamp=datetime.datetime.now(datetime.timezone.utc))
        assert removed >= len(keys)          # local eviction happened
        assert eb.get('pk0') == keys['pk0']  # transparently re-pulls
        eb['pk_new'] = b'new'
        assert eb.changes().push()

    with open_ebooklet(conn, local_path('prune-r'), flag='r') as eb:
        for k, v in keys.items():
            assert eb.get(k) == v            # remote untouched by prune
        assert eb.get('pk_new') == b'new'


#################################################
### Storage mode on unpushed reopens (journal-recorded since 0.10)
# The journal records the storage mode chosen at creation, so a bare reopen of a
# created-but-not-yet-pushed database keeps it (0.11: the target byte size is a
# writer setting - omitted, a grouped database reopens at DEFAULT_GROUP_BYTES).


def _remote_basenames(conn):
    with conn.open('w') as s3open:
        return sorted(obj['key'].split('/')[-1] for obj in s3open.list_objects().iter_objects())


def test_storage_mode_unpushed_reopen_reads_journal():
    """A bare 'w' reopen before the first push resolves the grouped mode from
    the journal. (The reopen warns about the pending REPLACEMENT - the 'n'
    session never pushed.)"""
    conn = make_conn('ngwarn')
    p = local_path('ng-warn-writer')

    with warnings.catch_warnings():
        warnings.simplefilter('error')  # fresh create: no warning expected
        with open_ebooklet(conn, p, flag='n', group_bytes=group_bytes) as eb:
            eb['key000'] = b'v0'
            # close WITHOUT pushing

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        with open_ebooklet(conn, p, flag='w') as eb:
            assert eb.group_bytes is not None  # journaled grouped mode
    assert [w for w in records if 'REPLACEMENT' in str(w.message)], \
        'unpushed replacement reopen should warn'

    # re-passing a byte target also works
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        with open_ebooklet(conn, p, flag='w', group_bytes=group_bytes) as eb:
            assert eb.group_bytes == group_bytes


def test_storage_mode_kept_and_pushed_remote_reopens_silently():
    """The first-push reopen keeps the grouped layout (journal-recorded);
    once pushed, bare reopens read the mode from the remote, silently."""
    conn = make_conn('ngrepass')
    p = local_path('ng-repass-writer')

    with open_ebooklet(conn, p, flag='n', group_bytes=group_bytes) as eb:
        eb['key000'] = b'v0'
        eb['key001'] = b'v1'

    with warnings.catch_warnings():
        warnings.simplefilter('ignore')  # pending-REPLACEMENT warning expected here
        with open_ebooklet(conn, p, flag='w', group_bytes=group_bytes) as eb:
            assert eb.group_bytes == group_bytes
            assert eb.changes().push()

    names = _remote_basenames(conn)
    assert 'key000' not in names                  # no raw per-key objects
    assert any(name.split('.')[0].isdigit() for name in names)  # group objects present

    vals, _ = read_all(conn, ['key000', 'key001'])
    assert vals == {'key000': b'v0', 'key001': b'v1'}

    # remote initialized + replacement completed -> bare reopen is silent and grouped
    with warnings.catch_warnings():
        warnings.simplefilter('error')
        with open_ebooklet(conn, p, flag='w') as eb:
            assert eb.group_bytes is not None


def test_storage_mode_unpushed_reopen_journal_rcg():
    """RemoteConnGroup flows through the same _init_common - same journal
    resolution (REPLACEMENT warning on the unpushed reopen)."""
    conn = make_conn('ngwarnrcg')
    p = local_path('ng-warn-rcg')
    with open_rcg(conn, p, flag='n', group_bytes=group_bytes) as rcg:
        pass

    with warnings.catch_warnings(record=True) as records:
        warnings.simplefilter('always')
        with open_rcg(conn, p, flag='w') as rcg:
            assert rcg.group_bytes is not None
    assert [w for w in records if 'REPLACEMENT' in str(w.message)]


#################################################
### 0.9.4: flag='n' sessions must wipe exactly once


def test_flag_n_second_push_preserves_remote():
    ## Data-loss regression: every push in a flag='n' session used to re-run the
    ## remote wipe and re-upload only the current changelog's groups - a second
    ## push destroyed everything the first push uploaded (silently pre-0.9.3,
    ## loudly after). The replacement intent must clear after the replacement
    ## push so the wipe happens exactly once (journal-backed since 0.10; _flag
    ## itself stays frozen).
    (k1, k2, k3), c1 = colliding_keys()
    conn = make_conn('nwipe')

    p = local_path('nwipe-writer')
    with open_ebooklet(conn, p, flag='n', group_bytes=group_bytes) as eb:
        eb[k1] = b'v1'
        eb[c1] = b'vc'
        eb.set_metadata({'m': 1})
        assert eb.changes().push()     # replacement push (wipes, uploads all)
        assert eb._flag == 'n'                              # frozen - no longer mutated
        assert eb._journal.replace_pending is False         # intent cleared: no further wipes

        eb[k2] = b'v2'                         # a second push in the same session
        assert eb.changes().push()     # second push must NOT wipe c1's group

    values, stored_keys = read_all(conn, [k1, k2, c1])
    assert values == {k1: b'v1', k2: b'v2', c1: b'vc'}   # c1 survived (was destroyed pre-fix)
    assert stored_keys == sorted([k1, k2, c1])

    p2 = local_path('nwipe-reader')
    with open_ebooklet(conn, p2, flag='r') as eb:
        assert eb.get_metadata() == {'m': 1}
