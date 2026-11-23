"""
The commit guard (0.12) and lock leases.

Once mutual exclusion is lost (a broken or expired lock), two writers can each
build a commit from the same base. Before 0.12 the later commit republished its
push-start manifest - naming generations the other writer's post-commit GC had
already deleted (an unreadable group), or silently dropping the other writer's
update - and both pushes reported success (review round lock-lease-plan-1, both
arms, on this code path). The guard: one HEAD of the db object through the write
session immediately before the commit PUT; a stamp other than the session's base
raises ConcurrentCommitError with everything journaled.

FakeLock grants every writer the lock, which is exactly the "mutual exclusion
lost" condition. Each race is run in both orders and for writers touching
different groups and the same group.
"""
import datetime
import logging

import pytest
import urllib3

from ebooklet import open_ebooklet, fsck, LockLostError, utils
from ebooklet.errors import ConcurrentCommitError
from ebooklet.main import _lock_timeout_message
from ebooklet.tests import fake_s3

## Two 1-char keys with 2-byte values fill one group (see test_write_order_groups).
GB2 = 40
SEED = ['a', 'b', 'c', 'd', 'e', 'f']           # groups 0: a b, 1: c d, 2: e f
UTC = datetime.timezone.utc


def _conn(store):
    return fake_s3.FakeS3Connection(store, 'd')


def _seed(store, tmp_path):
    with open_ebooklet(_conn(store), tmp_path / 'seed.blt', flag='n', group_bytes=GB2) as eb:
        for k in SEED:
            eb[k] = (k * 2).encode()
        assert eb.changes().push()


def _read_all(store, tmp_path, name):
    with open_ebooklet(_conn(store), tmp_path / name, flag='r') as r:
        return {k: r[k] for k in r.keys()}


@pytest.fixture(autouse=True)
def reset_fake_clock():
    yield
    fake_s3.FakeLock.clock = None


#################################################
### The commit guard: both orders x {different groups, same group}
##
## The dangerous interleaving (review round lock-lease-plan-1, Fable's eb_race):
## a writer has uploaded its groups and passed its PRE-COMMIT verify, then
## stalls while another writer runs a whole session; then its commit lands.
## (If the other writer commits before the stale push even starts, ebooklet's
## existing phase-A integrity check already fails the push for a shared group.)

CASES = {'different groups': ('a', 'f'), 'same group': ('a', 'b')}


def _during_precommit_verify(eb, fn):
    """Run fn() inside eb's pre-commit verify (its 2nd verify of the push), then pass."""
    real = eb.lock.verify
    calls = {'n': 0}

    def verify():
        calls['n'] += 1
        ok = real()
        if calls['n'] == 2:
            fn()
        return ok
    eb.lock.verify = verify
    return calls


def _other_writer_commits(store, tmp_path, name, key, value):
    def run():
        with open_ebooklet(_conn(store), tmp_path / name, flag='w', group_bytes=GB2) as o:
            o[key] = value
            assert o.changes().push()
    return run


@pytest.mark.parametrize('case', CASES)
def test_stale_commit_after_another_writers_commit_is_refused(tmp_path, case):
    """S stalls after its pre-commit verify; W commits; S's commit would
    republish S's stale manifest. S must be refused, W's commit survives."""
    s_key, w_key = CASES[case]
    store = {}
    _seed(store, tmp_path)
    s = open_ebooklet(_conn(store), tmp_path / 's.blt', flag='w', group_bytes=GB2)
    s[s_key] = b'SS'
    calls = _during_precommit_verify(s, _other_writer_commits(store, tmp_path, 'w.blt', w_key, b'WW'))
    with pytest.raises(ConcurrentCommitError, match='committed by another writer'):
        s.changes().push()
    assert calls['n'] == 2                                 # refused right after the pre-commit verify
    assert s_key in s._journal.written                     # S's change is kept for a retry
    s.close()
    data = _read_all(store, tmp_path, 'r1.blt')
    assert data[w_key] == b'WW' and data[s_key] == (s_key * 2).encode()
    assert fsck(_conn(store)).claimed_but_missing == []
    ## S's retry builds on W's commit: both changes land.
    with open_ebooklet(_conn(store), tmp_path / 's.blt', flag='w', group_bytes=GB2) as s2:
        assert s2.changes().push()
    data = _read_all(store, tmp_path, 'r2.blt')
    assert data[w_key] == b'WW' and data[s_key] == b'SS'
    assert fsck(_conn(store)).claimed_but_missing == []


@pytest.mark.parametrize('case', CASES)
def test_stale_writer_refused_whichever_key_it_holds(tmp_path, case):
    """The same interleaving with the roles' keys swapped: the writer stalled at
    its pre-commit verify (here W) is refused, the one that committed (S) survives."""
    s_key, w_key = CASES[case]
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2)
    w[w_key] = b'WW'
    _during_precommit_verify(w, _other_writer_commits(store, tmp_path, 's.blt', s_key, b'SS'))
    with pytest.raises(ConcurrentCommitError):
        w.changes().push()
    w.close()
    data = _read_all(store, tmp_path, 'r1.blt')
    assert data[s_key] == b'SS' and data[w_key] == (w_key * 2).encode()
    assert fsck(_conn(store)).claimed_but_missing == []


def test_a_remote_created_after_an_absent_open_is_refused_at_push_start(tmp_path):
    """Caught before the commit guard, by the pre-push uuid check (the guard's
    own 'appeared' branch is tested separately below)."""
    from ebooklet.errors import UUIDMismatchError
    store = {}
    a = open_ebooklet(_conn(store), tmp_path / 'a.blt', flag='c', group_bytes=GB2)   # remote absent
    a['x'] = b'AA'
    with open_ebooklet(_conn(store), tmp_path / 'b.blt', flag='c', group_bytes=GB2) as b:
        b['y'] = b'BB'
        assert b.changes().push()                                                     # created it
    with pytest.raises(UUIDMismatchError):
        a.changes().push()
    a.close()
    assert _read_all(store, tmp_path, 'r.blt') == {'y': b'BB'}


def test_ordinary_pushes_pass_the_guard(tmp_path):
    """One session, several pushes: each commit refreshes the base, so the
    guard never fires on a writer's own commits."""
    store = {}
    _seed(store, tmp_path)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as w:
        for i in range(3):
            w[f'n{i}'] = b'NN'
            assert w.changes().push()


#################################################
### Cleanup after the commit re-verifies


def test_gc_is_skipped_when_the_lock_is_lost_after_the_commit(tmp_path, caplog):
    store = {}
    _seed(store, tmp_path)
    old = {k for k in store if k.startswith('d/0.')}
    assert len(old) == 1
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2)
    w['a'] = b'ZZ'                                     # repacks group 0: its old generation is GC'd normally
    calls = {'n': 0}

    def verify():
        calls['n'] += 1
        return calls['n'] <= 2                         # push start and pre-commit pass; the pre-GC check fails
    w.lock.verify = verify
    with caplog.at_level(logging.WARNING):
        assert w.changes().push()
    w.close()
    assert old <= set(store)                           # old generation NOT deleted
    assert 'skipping the GC' in caplog.text
    assert _read_all(store, tmp_path, 'r.blt')['a'] == b'ZZ'


#################################################
### Leases


def test_lease_refused_for_per_key_storage(tmp_path):
    store = {}
    with pytest.raises(ValueError, match='per-key'):
        open_ebooklet(_conn(store), tmp_path / 'p.blt', flag='c', group_bytes=None, lock_lease=1800)
    with open_ebooklet(_conn(store), tmp_path / 'p.blt', flag='n', group_bytes=None) as p:   # a per-key remote
        p['a'] = b'aa'
        assert p.changes().push()
    with pytest.raises(ValueError, match='per-key'):                                          # inherited mode
        open_ebooklet(_conn(store), tmp_path / 'p2.blt', flag='w', lock_lease=1800)


def test_lease_refused_for_replacement_sessions(tmp_path):
    with pytest.raises(ValueError, match="flag='n'"):
        open_ebooklet(_conn({}), tmp_path / 'n.blt', flag='n', group_bytes=GB2, lock_lease=1800)


def test_leased_commit_is_fenced_at_the_lease_deadline(tmp_path):
    store = {}
    _seed(store, tmp_path)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800) as w:
        assert w.lock.lease == 1800
        w['a'] = b'LL'
        assert w.changes().push()
        fenced = w._remote_session._write_session.fenced_puts
        assert [k for k, _ in fenced] == ['d']                        # only the commit PUT is fenced
        assert fenced[0][1] == w.lock.lease_deadline
    assert _read_all(store, tmp_path, 'r.blt')['a'] == b'LL'


def test_unleased_commit_is_not_fenced(tmp_path):
    store = {}
    _seed(store, tmp_path)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as w:
        w['a'] = b'UU'
        assert w.changes().push()
        assert w._remote_session._write_session.fenced_puts == []


def test_a_commit_refused_by_the_server_keeps_the_journal(tmp_path):
    """The lease runs out between the pre-commit verify and the commit PUT: the
    server refuses the fenced PUT, nothing is committed, the change is kept."""
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800)
    w['a'] = b'LL'
    real_verify = w.lock.verify
    calls = {'n': 0}

    def verify_then_stall():
        calls['n'] += 1
        ok = real_verify()
        if calls['n'] == 2:   # the pre-commit verify passes, THEN the process stalls past the deadline
            fake_s3.FakeLock.clock = lambda: datetime.datetime.now(UTC) + datetime.timedelta(seconds=1800)
        return ok
    w.lock.verify = verify_then_stall
    with pytest.raises(LockLostError, match='refused by the server'):
        w.changes().push()
    assert 'a' in w._journal.written
    w.close()
    fake_s3.FakeLock.clock = None
    assert _read_all(store, tmp_path, 'r1.blt')['a'] == b'aa'         # nothing committed
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800) as w2:
        assert w2.changes().push()
    assert _read_all(store, tmp_path, 'r2.blt')['a'] == b'LL'


def test_push_after_the_lease_ran_out_names_the_reason(tmp_path):
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800)
    w['a'] = b'LL'
    fake_s3.FakeLock.clock = lambda: datetime.datetime.now(UTC) + datetime.timedelta(seconds=1800)
    with pytest.raises(LockLostError, match=r'lease \(1800 s\) has run out'):
        w.changes().push()
    w.close()


#################################################
### Messages


class _StubLock:
    def __init__(self, others):
        self._others = others

    def other_locks(self):
        return self._others


def test_timeout_message_reports_each_blockers_lease():
    now = datetime.datetime.now(UTC)
    msg = _lock_timeout_message(_StubLock({
        'aaaaaaaaaaaaal1800': {'upload_timestamp': now - datetime.timedelta(seconds=600), 'lease': 1800,
                               'expires_at': now + datetime.timedelta(seconds=1260)},
        'bbbbbbbbbbbbb': {'upload_timestamp': now - datetime.timedelta(hours=3), 'lease': None, 'expires_at': None},
        'ccccccccccccel200': {'upload_timestamp': now - datetime.timedelta(seconds=400), 'lease': 200,
                              'expires_at': now - datetime.timedelta(seconds=140)},
    }), 300)
    assert 'lease 1800s (breakable in about 12' in msg
    assert 'no lease (a legacy or unleased writer: never auto-broken)' in msg
    assert 'lease 200s (breakable now (expired))' in msg
    assert 'force_lock=True' in msg
    assert 'tickets older than 2 hours' in _lock_timeout_message(_StubLock({}), 300)


def test_lock_lost_message_names_the_reason():
    lock = fake_s3.FakeLock(lease=1800)
    for reason, text in [('lease_expired', 'lease (1800 s) has run out'), ('no_server_date', 'stopped reporting its clock'),
                         ('broken', 'was broken by another client'), (None, 'was broken by another client')]:
        lock.lost_reason = reason
        assert text in utils.lock_lost_message(lock, 'before the commit')


def test_leased_session_refuses_to_push_if_the_remote_turned_per_key(tmp_path):
    """A re-pull can switch the session's storage mode (another writer replaced
    the remote); a leased session must then refuse to push per-key."""
    store = {}
    _seed(store, tmp_path)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800) as w:
        w['a'] = b'LL'
        w._group_bytes = None                      # what a re-pull of a now per-key remote sets
        with pytest.raises(ValueError, match='now per-key'):
            w.changes().push()


#################################################
### Review round ebooklet-lease-code-1


def _put_that_stores_then(eb, outcome, times=1):
    """Make the session's db-object PUT store the commit, then fail (`outcome`: 'raise' or an HTTP status)."""
    ws = eb._remote_session._write_session
    real = ws.put_object
    state = {'left': times}

    def put(key, obj, metadata=None, content_type=None, **kw):
        resp = real(key, obj, metadata=metadata, content_type=content_type, **kw)
        if key == 'd' and state['left'] > 0:
            state['left'] -= 1
            if outcome == 'raise':
                raise urllib3.exceptions.ProtocolError('injected: response lost after the store')
            return fake_s3.FakeResp(outcome, error={'Code': 'InternalError', 'Message': 'injected'})
        return resp
    ws.put_object = put


@pytest.mark.parametrize('outcome', ['raise', 500, 403])
def test_a_landed_commit_whose_response_failed_counts_as_committed(tmp_path, outcome):
    store = {}
    _seed(store, tmp_path)
    lease = {'lock_lease': 1800} if outcome == 403 else {}
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, **lease) as w:
        w['a'] = b'LL'
        _put_that_stores_then(w, outcome)
        assert w.changes().push()
        assert 'a' not in w._journal.written                 # cleared: it IS committed
        w['b'] = b'L2'
        assert w.changes().push()                             # and the base moved on with it
    data = _read_all(store, tmp_path, 'r.blt')
    assert data['a'] == b'LL' and data['b'] == b'L2'
    assert fsck(_conn(store)).claimed_but_missing == []


def test_a_retry_after_an_undetected_landed_commit_is_not_refused(tmp_path):
    """The PUT stored and raised, and the HEAD that would have noticed failed too:
    the push raises with the journal kept. The documented retry must not be
    refused as 'another writer' - the remote carries this session's own commit."""
    store = {}
    _seed(store, tmp_path)
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2) as w:
        w['a'] = b'LL'
        _put_that_stores_then(w, 'raise')
        rs = w._remote_session
        real_ts = rs.get_timestamp_writer
        calls = {'n': 0}

        def ts():
            calls['n'] += 1
            if calls['n'] == 2:                                # the post-failure HEAD
                raise urllib3.exceptions.ProtocolError('injected: HEAD failed too')
            return real_ts()
        rs.get_timestamp_writer = ts
        with pytest.raises(urllib3.exceptions.ProtocolError):
            w.changes().push()
        assert 'a' in w._journal.written
        assert w.changes().push(force_push=True)
    data = _read_all(store, tmp_path, 'r.blt')
    assert data['a'] == b'LL'
    assert fsck(_conn(store)).claimed_but_missing == []


def test_the_guard_base_cannot_be_moved_by_a_repull_during_the_push(tmp_path):
    """Another writer commits while S is stalled; a re-pull in S's session (a
    reader thread resolving a missing key does this) then loads that commit's
    metadata. S's commit is still built on the old state and must be refused."""
    store = {}
    _seed(store, tmp_path)
    s = open_ebooklet(_conn(store), tmp_path / 's.blt', flag='w', group_bytes=GB2)
    s['a'] = b'SS'

    def other_then_repull():
        _other_writer_commits(store, tmp_path, 'w.blt', 'f', b'WW')()
        s._remote_session._load_db_metadata()                 # what _pull_remote_index does
    _during_precommit_verify(s, other_then_repull)
    with pytest.raises(ConcurrentCommitError):
        s.changes().push()
    s.close()
    data = _read_all(store, tmp_path, 'r.blt')
    assert data['f'] == b'WW' and data['a'] == b'aa'
    assert fsck(_conn(store)).claimed_but_missing == []


def test_guard_refuses_a_remote_that_vanished(tmp_path):
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2)
    w['a'] = b'LL'
    _during_precommit_verify(w, lambda: store.pop('d'))
    with pytest.raises(ConcurrentCommitError, match='remote commit stamp None'):
        w.changes().push()
    w.close()


def _other_writer_creates(store, tmp_path):
    def run():
        with open_ebooklet(_conn(store), tmp_path / 'b.blt', flag='c', group_bytes=GB2) as b:
            b['y'] = b'BB'
            assert b.changes().push()
    return run


def test_guard_refuses_a_remote_that_appeared(tmp_path):
    store = {}
    a = open_ebooklet(_conn(store), tmp_path / 'a.blt', flag='c', group_bytes=GB2)   # absent at open AND at push start
    a['x'] = b'AA'
    _during_precommit_verify(a, _other_writer_creates(store, tmp_path))
    with pytest.raises(ConcurrentCommitError, match='this session is based on None'):
        a.changes().push()
    a.close()
    assert _read_all(store, tmp_path, 'r.blt') == {'y': b'BB'}


class _CachedReadConnection(fake_s3.FakeS3Connection):
    """A writer whose READ session answers HEADs of the db object from a cache
    frozen at open (a caching CDN in front of db_url); its WRITE session is fresh."""
    def open(self, flag='r'):
        if flag == 'r':
            return super().open(flag)
        from ebooklet import remote
        write = fake_s3.FakeS3Session(self.store, bucket=self.bucket)
        read = fake_s3.FakeS3Session(self.store, bucket=self.bucket)
        frozen = read.head_object(self.db_key)
        real_head = read.head_object
        read.head_object = lambda key, version_id=None: frozen if key == self.db_key else real_head(key)
        return remote.S3SessionWriter(read, write, self.db_key, self.db_key, self.threads)


def test_the_guard_heads_through_the_write_session(tmp_path):
    """With the read session's HEAD frozen at the base, only a WRITE-session
    HEAD can see the other writer's commit."""
    store = {}
    _seed(store, tmp_path)
    s = open_ebooklet(_CachedReadConnection(store, 'd'), tmp_path / 's.blt', flag='w', group_bytes=GB2)
    s['a'] = b'SS'
    _during_precommit_verify(s, _other_writer_commits(store, tmp_path, 'w.blt', 'f', b'WW'))
    with pytest.raises(ConcurrentCommitError):
        s.changes().push()
    s.close()
    assert fsck(_conn(store)).claimed_but_missing == []


def _record_releases():
    released = []
    orig = fake_s3.FakeLock.release

    def release(self):
        released.append(True)
        orig(self)
    return released, orig, release


def test_lease_refused_over_a_journaled_replacement(tmp_path):
    store = {}
    _seed(store, tmp_path)
    with open_ebooklet(_conn(store), tmp_path / 'n.blt', flag='n', group_bytes=GB2) as n:
        n['x'] = b'XX'                                        # closed unpushed: replace_pending
    released, orig, release = _record_releases()
    fake_s3.FakeLock.release = release
    try:
        with pytest.raises(ValueError, match="unpushed flag='n' replacement"):
            open_ebooklet(_conn(store), tmp_path / 'n.blt', flag='w', group_bytes=GB2, lock_lease=1800)
    finally:
        fake_s3.FakeLock.release = orig
    assert released                                            # the refused open released its lock
    with open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800) as w:
        w._journal.set_replace_pending(True)                   # the push-time check
        with pytest.raises(ValueError, match='replacement push'):
            w.changes().push()


def test_refused_per_key_open_releases_its_lock(tmp_path):
    released, orig, release = _record_releases()
    fake_s3.FakeLock.release = release
    try:
        with pytest.raises(ValueError, match='per-key'):
            open_ebooklet(_conn({}), tmp_path / 'p.blt', flag='c', group_bytes=None, lock_lease=1800)
    finally:
        fake_s3.FakeLock.release = orig
    assert released


def test_lease_refused_for_read_only_sessions(tmp_path):
    store = {}
    _seed(store, tmp_path)
    with pytest.raises(ValueError, match="flag='r'"):
        open_ebooklet(_conn(store), tmp_path / 'r.blt', flag='r', lock_lease=1800)


def test_push_time_per_key_check_runs_after_adoption(tmp_path):
    """A leased session opened against an absent remote; another writer with the
    same local file (a copy, so the same uuid) then creates the remote PER-KEY.
    The push adopts the remote - and with it per-key mode - so it must refuse."""
    import shutil
    store = {}
    a = open_ebooklet(_conn(store), tmp_path / 'a.blt', flag='c', group_bytes=GB2, lock_lease=1800)
    a['x'] = b'AA'
    a.sync()
    shutil.copy(tmp_path / 'a.blt', tmp_path / 'b.blt')
    with open_ebooklet(_conn(store), tmp_path / 'b.blt', flag='w', group_bytes=None) as b:
        b['y'] = b'BB'
        assert b.changes().push()
    with pytest.raises(ValueError, match='now per-key'):
        a.changes().push()
    a.close()


def test_open_rcg_passes_the_lease(tmp_path):
    from ebooklet import open_rcg
    store = {}
    _seed(store, tmp_path)                                              # a member database at 'd'
    with open_rcg(fake_s3.FakeS3Connection(store, 'cat'), tmp_path / 'g.rcg', flag='n') as g:
        g.add(_conn(store), key='m1')
        assert g.changes().push()
    with open_rcg(fake_s3.FakeS3Connection(store, 'cat'), tmp_path / 'g.rcg', flag='w', lock_lease=1800) as g:
        assert g.lock.lease == 1800
        g.add(_conn(store), key='m2')
        assert g.changes().push()
        assert [k for k, _ in g._remote_session._write_session.fenced_puts] == ['cat']


def test_a_post_commit_verify_failure_does_not_fail_the_push(tmp_path, caplog):
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2)
    w['a'] = b'ZZ'
    calls = {'n': 0}
    real = w.lock.verify

    def verify():
        calls['n'] += 1
        if calls['n'] == 3:                                    # the pre-GC check
            raise urllib3.exceptions.HTTPError('injected: listing failed')
        return real()
    w.lock.verify = verify
    with caplog.at_level(logging.WARNING):
        assert w.changes().push()
    assert 'Could not re-verify the write lock after the commit' in caplog.text
    w.close()
    assert _read_all(store, tmp_path, 'r.blt')['a'] == b'ZZ'


def test_a_local_lease_refusal_is_a_lock_lost_error(tmp_path):
    import s3func
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800)
    w['a'] = b'LL'
    ws = w._remote_session._write_session
    real = ws.put_object

    def put(key, obj, metadata=None, content_type=None, expires_at=None):
        if key == 'd':
            raise s3func.LeaseExpiredError('PUT not sent: its deadline has passed')
        return real(key, obj, metadata=metadata, content_type=content_type, expires_at=expires_at)
    ws.put_object = put
    with pytest.raises(LockLostError, match='ran out before the commit could be sent'):
        w.changes().push()
    assert 'a' in w._journal.written
    w.close()


@pytest.mark.parametrize('status', [401, 403])
def test_a_refused_fenced_commit_is_a_lease_expiry_only_if_the_lease_ran_out(tmp_path, status):
    store = {}
    _seed(store, tmp_path)
    w = open_ebooklet(_conn(store), tmp_path / 'w.blt', flag='w', group_bytes=GB2, lock_lease=1800)
    w['a'] = b'LL'
    ws = w._remote_session._write_session
    real = ws.put_object

    def refuse(key, obj, metadata=None, content_type=None, expires_at=None):
        if key == 'd':
            return fake_s3.FakeResp(status, error={'Code': 'AccessDenied', 'Message': 'key not valid'})
        return real(key, obj, metadata=metadata, content_type=content_type, expires_at=expires_at)
    ws.put_object = refuse
    ## Lease still valid: a credential problem, not an expiry.
    with pytest.raises(urllib3.exceptions.HTTPError, match='not a lease expiry'):
        w.changes().push()
    assert 'a' in w._journal.written
    ## Lease run out: an expiry.
    fake_s3.FakeLock.clock = lambda: datetime.datetime.now(UTC) + datetime.timedelta(seconds=1800)
    real_verify = w.lock.verify
    n = {'c': 0}

    def verify():                                               # let the push-start and pre-commit checks pass
        n['c'] += 1
        return True if n['c'] <= 2 else real_verify()
    w.lock.verify = verify
    with pytest.raises(LockLostError, match=r'refused by the server \(HTTP %d\)' % status):
        w.changes().push()
    w.close()
