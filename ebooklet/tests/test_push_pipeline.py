"""
Hermetic tests for the 0.10.1 pipelined push: the changelog-sweep offset
capture (loc_map), private-fd elevator reads in phase B, the pulled-keys
locked-read precedence (a pulled newer value must beat a stale captured
offset), the pack read-gate (push_packers), the prune/clear-during-push
guards and the compaction belt (ConcurrentCompactionError before commit),
the 'ebooklet.push' progress records, and the locked-read fallback path.
"""
import logging
import re
import struct
import threading
import time

import pytest

from ebooklet import (
    open_ebooklet,
    open_rcg,
    utils,
    PushInProgressError,
    ConcurrentCompactionError,
)
from ebooklet.tests import fake_s3

NUM_GROUPS = 5


def _seed(store, db_key, tmp_path, name='seed.blt', items=None, num_groups=NUM_GROUPS):
    conn = fake_s3.FakeS3Connection(store, db_key)
    with open_ebooklet(conn, tmp_path / name, flag='n', num_groups=num_groups) as eb:
        for k, v in (items or {'k1': b'v1', 'k2': b'v2'}).items():
            eb[k] = v
        assert eb.changes().push()
    return conn


def _group_objects(store, db_key='testdb'):
    return sorted(k for k in store if re.match(rf'{db_key}/\d+\.[0-9a-f]{{13}}$', k))


def _unpack_group(data):
    """Parse a packed group object into {key: (ts_int, value_bytes)}."""
    n = struct.unpack_from('>I', data, 0)[0]
    pos = 4
    out = {}
    for _ in range(n):
        klen = struct.unpack_from('>H', data, pos)[0]
        pos += 2
        key = data[pos:pos + klen].decode()
        pos += klen
        ts = utils.bytes_to_int(data[pos:pos + 7])
        pos += 7
        vlen = struct.unpack_from('>I', data, pos)[0]
        pos += 4
        out[key] = (ts, data[pos:pos + vlen])
        pos += vlen
    assert pos == len(data), 'group object has trailing bytes'
    return out


def test_pipelined_push_content_identical(tmp_path):
    """Every live generation object's entries match the pushed data; every
    sidecar index entry resolves (ts, offset, len) into the right value bytes;
    and the staged ts equals the packed ts by construction."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * (i + 3) for i in range(25)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)

    expected = dict(items)
    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        for i in (0, 7, 13):
            expected[f'k{i}'] = f'CHANGED-{i}'.encode()
            eb[f'k{i}'] = expected[f'k{i}']
        assert eb.changes().push()

        ## Unpack every live generation object: full content equality.
        manifest = utils.parse_db_payload(store['testdb'][0])[0]
        packed_all = {}
        for gid, gen in manifest.items():
            data = store[f'testdb/{utils.group_obj_key(gid, gen)}'][0]
            for key, (ts, val) in _unpack_group(data).items():
                assert utils.key_to_group_id(key, NUM_GROUPS) == gid
                packed_all[key] = (ts, val, gid, gen)
        assert {k: v[1] for k, v in packed_all.items()} == expected

        ## Every index entry slices its group object to the right bytes, and
        ## the staged ts is identical to the packed ts.
        for key in expected:
            entry = eb._remote_index[key]
            ts = utils.bytes_to_int(entry[:7])
            offset = utils.bytes_to_int(entry[7:11])
            length = utils.bytes_to_int(entry[11:15])
            p_ts, p_val, gid, gen = packed_all[key]
            assert ts == p_ts, f'{key}: staged ts != packed ts'
            data = store[f'testdb/{utils.group_obj_key(gid, gen)}'][0]
            assert data[offset:offset + length] == expected[key]
    finally:
        eb.close()

    ## A fresh reader (the real consumer path) agrees.
    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        for k, v in expected.items():
            assert r[k] == v


def test_pull_precedence_stale_local(tmp_path):
    """The F1 review scenario: a key that is locally STALE (newer value on the
    remote) is pulled in phase A and the pulled value must be packed - the
    stale captured offset must never win."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'a.blt',
                 items={'shared': b'v1-old', 'sibling': b's1'})

    ## Client B materializes the ORIGINAL value locally, then closes.
    conn_b = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(conn_b, tmp_path / 'b.blt', flag='r') as rb:
        assert rb['shared'] == b'v1-old'

    ## Client A pushes a NEWER value for the shared key.
    with open_ebooklet(conn, tmp_path / 'a.blt', flag='w') as ea:
        ea['shared'] = b'v2-NEWER'
        assert ea.changes().push()

    ## Client B (stale local copy of 'shared') edits only the sibling and
    ## pushes: phase A pulls the newer 'shared', which must be what repacks.
    with open_ebooklet(conn_b, tmp_path / 'b.blt', flag='w') as eb:
        assert eb._local_file.get_timestamp('shared', include_value=True, decode_value=False)[1] == b'v1-old'
        eb['sibling'] = b's2'
        result = eb.changes().push()
        assert result, result.failures

    manifest = utils.parse_db_payload(store['testdb'][0])[0]
    packed = {}
    for gid, gen in manifest.items():
        packed.update({k: v for k, (t, v) in _unpack_group(store[f'testdb/{utils.group_obj_key(gid, gen)}'][0]).items()})
    assert packed['shared'] == b'v2-NEWER', 'stale captured offset won over the pulled value'
    assert packed['sibling'] == b's2'

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'c.blt', flag='r') as r:
        assert r['shared'] == b'v2-NEWER'
        assert r['sibling'] == b's2'


def test_progress_records(tmp_path, caplog):
    """One start + one per group + one summary + one commit record on the
    'ebooklet.push' logger, with byte totals that agree exactly."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * 50 for i in range(15)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        for i in range(15):
            eb[f'k{i}'] = bytes([97 + i]) * 60   # touch every group
        with caplog.at_level(logging.INFO, logger='ebooklet.push'):
            caplog.clear()
            assert eb.changes().push()
    finally:
        eb.close()

    records = [r for r in caplog.records if r.name == 'ebooklet.push']
    msgs = [r.getMessage() for r in records]
    starts = [m for m in msgs if m.startswith('push upload starting')]
    groups = [m for m in msgs if m.startswith('group ')]
    summaries = [m for m in msgs if m.startswith('push upload finished')]
    commits = [m for m in msgs if m.startswith('commit succeeded')]
    assert len(starts) == 1 and len(summaries) == 1 and len(commits) == 1
    ## The affected-group count depends on key hashing (15 keys may land in
    ## fewer than NUM_GROUPS groups) - the start record is the authority.
    n_groups = int(re.search(r'starting: (\d+) group', starts[0]).group(1))
    assert 1 <= n_groups <= NUM_GROUPS
    assert len(groups) == n_groups
    assert all(r.levelno == logging.INFO for r in records)

    ## The upfront total (from the captured lengths + pack overhead) must
    ## equal the sum of the actually-packed bytes, exactly.
    total = int(re.search(r'([\d,]+) bytes', starts[0]).group(1).replace(',', ''))
    per_group = [int(re.search(r': ([\d,]+) B \(pack', m).group(1).replace(',', '')) for m in groups]
    assert total == sum(per_group)
    assert f'{n_groups}/{n_groups} group(s)' in summaries[0]
    assert '0 failure(s)' in summaries[0]


def test_prune_and_clear_during_push_raise(tmp_path):
    """The session-level guard: prune()/clear() raise while a push runs."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt')

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        ## Direct unit: the flag alone gates both entry points.
        eb._push_active = True
        with pytest.raises(PushInProgressError):
            eb.prune()
        with pytest.raises(PushInProgressError):
            eb.clear()
        eb._push_active = False

        ## Threaded: a real push blocked mid-upload rejects a concurrent prune.
        eb['k1'] = b'v1-changed'
        session = eb._remote_session._write_session
        orig_put = session.put_object
        put_started = threading.Event()
        release_put = threading.Event()

        def blocking_put(key, data, metadata=None):
            if re.match(r'testdb/\d+\.[0-9a-f]{13}$', key):
                put_started.set()
                assert release_put.wait(timeout=30), 'test deadlock: release never set'
            return orig_put(key, data, metadata)

        session.put_object = blocking_put
        results = {}

        def run_push():
            results['push'] = eb.changes().push()

        t = threading.Thread(target=run_push)
        t.start()
        try:
            assert put_started.wait(timeout=30), 'push never reached the group PUT'
            with pytest.raises(PushInProgressError):
                eb.prune()
        finally:
            release_put.set()
            t.join(timeout=30)
        assert not t.is_alive()
        assert results['push']

        ## After the push, prune works again.
        session.put_object = orig_put
        eb.prune()
    finally:
        eb.close()


def test_compaction_mid_push_aborts_before_commit(tmp_path, monkeypatch):
    """The belt: an out-of-band compaction (direct booklet prune, bypassing
    the session guard) between the capture and the pack aborts the push with
    ConcurrentCompactionError BEFORE the commit - the remote db object is
    byte-unchanged, the journal intact, and a retry converges."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * 40 for i in range(10)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)
    db_object_before = store['testdb']

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        eb['k0'] = b'CHANGED'
        eb['k0-old'] = b'x'
        del eb['k0-old']   # dead block so the out-of-band prune has work

        orig_read = utils._read_group_values
        fired = []

        def compact_after_read(local_file, entries, pulled_keys, fallback_warned):
            out = orig_read(local_file, entries, pulled_keys, fallback_warned)
            if not fired:
                fired.append(1)
                local_file.prune()   # out-of-band: bypasses the session guard
            return out

        monkeypatch.setattr(utils, '_read_group_values', compact_after_read)
        with pytest.raises(ConcurrentCompactionError):
            eb.changes().push()
        monkeypatch.setattr(utils, '_read_group_values', orig_read)

        ## Nothing committed: the remote db object is byte-identical and the
        ## pending write is still journaled.
        assert store['testdb'] is db_object_before or store['testdb'][0] == db_object_before[0]
        assert 'k0' in eb._journal.written

        ## The retry captures fresh (post-prune) offsets and converges.
        result = eb.changes().push()
        assert result, result.failures
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        assert r['k0'] == b'CHANGED'


def test_pack_failure_flows_to_pushresult(tmp_path, monkeypatch):
    """A pack-side exception surfaces through the (error, ...) worker channel
    into PushResult.failures; the other groups commit and the failed group's
    pending writes stay journaled for the retry."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * 20 for i in range(10)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        changed = {}
        for i in range(10):
            changed[f'k{i}'] = f'NEW-{i}'.encode()
            eb[f'k{i}'] = changed[f'k{i}']

        orig_pack = utils.pack_group
        calls = []

        def flaky_pack(entries):
            if not calls:
                calls.append(1)
                raise RuntimeError('induced pack failure')
            return orig_pack(entries)

        monkeypatch.setattr(utils, 'pack_group', flaky_pack)
        result = eb.changes().push()
        monkeypatch.setattr(utils, 'pack_group', orig_pack)

        assert not result
        assert result.updated, 'the other groups should have committed'
        assert len(result.failures) == 1
        (failed_gid, msg), = result.failures.items()
        assert 'RuntimeError: induced pack failure' in msg
        failed_keys = {k for k in changed if utils.key_to_group_id(k, NUM_GROUPS) == failed_gid}
        assert failed_keys, 'failures key is not a group id'
        for k in failed_keys:
            assert k in eb._journal.written, 'failed group keys must stay journaled'
        for k in set(changed) - failed_keys:
            assert k not in eb._journal.written, 'committed keys must clear from the journal'

        ## Retry converges.
        result = eb.changes().push()
        assert result, result.failures
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        for k, v in changed.items():
            assert r[k] == v


def test_upload_failure_mid_stream(tmp_path):
    """A failed group PUT lands in failures; no index entries stage for that
    group; the retry converges."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * 20 for i in range(10)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)
    gid0 = utils.key_to_group_id('k0', NUM_GROUPS)

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        entry_before = eb._remote_index['k0']
        for i in range(10):
            eb[f'k{i}'] = f'NEW-{i}'.encode()

        session = eb._remote_session._write_session
        orig_put = session.put_object

        def failing_put(key, data, metadata=None):
            if re.match(rf'testdb/{gid0}\.[0-9a-f]{{13}}$', key):
                return fake_s3.FakeResp(status=500, error={'message': 'induced upload failure'})
            return orig_put(key, data, metadata)

        session.put_object = failing_put
        result = eb.changes().push()
        assert not result
        assert set(result.failures) == {gid0}
        ## The failed group's sidecar entry never staged.
        assert eb._remote_index['k0'] == entry_before

        session.put_object = orig_put
        result = eb.changes().push()
        assert result, result.failures
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        for i in range(10):
            assert r[f'k{i}'] == f'NEW-{i}'.encode()


def test_locked_fallback_path(tmp_path, monkeypatch, caplog):
    """The private-fd fast path denied (the Windows mandatory-lock case,
    injected via OSError on POSIX): the push falls back to locked reads,
    produces identical output, and warns once."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * 30 for i in range(12)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)

    def denied(path):
        raise OSError('induced mandatory-lock denial')

    monkeypatch.setattr(utils, '_open_private_reader', denied)

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w')
    try:
        changed = {f'k{i}': f'NEW-{i}'.encode() for i in range(12)}
        for k, v in changed.items():
            eb[k] = v
        with caplog.at_level(logging.INFO, logger='ebooklet.push'):
            caplog.clear()
            result = eb.changes().push()
        assert result, result.failures
        fallback_warnings = [r for r in caplog.records
                             if r.name == 'ebooklet.push' and 'fast (private-fd) read path' in r.getMessage()]
        assert len(fallback_warnings) == 1, 'the fallback must warn exactly once per push'
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        for k, v in changed.items():
            assert r[k] == v


def test_push_packers_plumbing(tmp_path):
    """push_packers plumbs from both factories and validates >= 1."""
    store = {}
    conn = _seed(store, 'testdb', tmp_path, 'seed.blt')

    with open_ebooklet(conn, tmp_path / 'p3.blt', flag='r', push_packers=3) as eb:
        assert eb._push_packers == 3
    with pytest.raises(ValueError, match='push_packers'):
        open_ebooklet(conn, tmp_path / 'p0.blt', flag='r', push_packers=0)

    rcg_conn = fake_s3.FakeS3Connection(store, 'rcgdb')
    with open_rcg(rcg_conn, tmp_path / 'rcg.blt', flag='n', num_groups=3, push_packers=2) as rcg:
        assert rcg._push_packers == 2
        assert rcg.changes().push()
    with pytest.raises(ValueError, match='push_packers'):
        open_rcg(rcg_conn, tmp_path / 'rcg0.blt', flag='r', push_packers=0)


def test_pack_group_is_linear():
    """Tripwire for the 0.10.0 quadratic bytes-concatenation regression:
    packing a production-sized group (~90MB, 600 members) must be near-
    instant. The quadratic version did ~100GB of memcpy here (~60s) - the
    primary cause of the one-group-per-~105s production push cadence. The
    5s bound is ~50x the linear cost on slow hardware, so it only trips on
    a genuine complexity regression."""
    value = b'x' * 150_000
    entries = [(f'key{i:06d}', 1_600_000_000_000_000 + i, value) for i in range(600)]
    t0 = time.monotonic()
    packed, offsets = utils.pack_group(entries)
    dt = time.monotonic() - t0
    assert dt < 5.0, f'pack_group took {dt:.1f}s for ~90MB - quadratic concatenation is back'
    assert isinstance(packed, bytes)
    assert len(packed) == 4 + sum(2 + len(k.encode()) + 7 + 4 + len(v) for k, _t, v in entries)
    ## offsets slice the payload to the right bytes
    for key in ('key000000', 'key000599'):
        off, ln = offsets[key]
        assert packed[off:off + ln] == value


def test_wider_gate_and_multiple_packers(tmp_path):
    """A gate wider than one (and wider than the group count) behaves
    identically - content equality with push_packers=4."""
    store = {}
    items = {f'k{i}': bytes([65 + i]) * 25 for i in range(20)}
    conn = _seed(store, 'testdb', tmp_path, 'w.blt', items=items)

    eb = open_ebooklet(conn, tmp_path / 'w.blt', flag='w', push_packers=4)
    try:
        changed = {f'k{i}': f'W-{i}'.encode() for i in range(20)}
        for k, v in changed.items():
            eb[k] = v
        assert eb.changes().push()
    finally:
        eb.close()

    fresh = fake_s3.FakeS3Connection(store, 'testdb')
    with open_ebooklet(fresh, tmp_path / 'fresh.blt', flag='r') as r:
        for k, v in changed.items():
            assert r[k] == v
