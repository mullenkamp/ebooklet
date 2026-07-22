import datetime
import pytest
import pathlib
import booklet
from ebooklet import utils, remote, open_ebooklet
from ebooklet.tests import fake_s3
import uuid6 as uuid


def test_key_to_group_id():
    # Deterministic
    assert utils.key_to_group_id('test', 10) == utils.key_to_group_id('test', 10)
    # Different keys can map to different groups
    results = set(utils.key_to_group_id(str(i), 10) for i in range(100))
    assert len(results) > 1
    # Always within range
    for i in range(100):
        gid = utils.key_to_group_id(str(i), 10)
        assert 0 <= gid < 10


def test_pack_unpack_roundtrip():
    entries = [
        ('key1', 1000000, b'value1'),
        ('key2', 2000000, b'value2'),
        ('key3', 3000000, b'a longer value here'),
    ]
    packed, offsets = utils.pack_group(entries)
    unpacked = utils.unpack_group(packed)
    assert len(unpacked) == len(entries)
    for (ok, ot, ov), (uk, ut, uv) in zip(entries, unpacked):
        assert ok == uk
        assert ot == ut
        assert ov == uv


def test_pack_unpack_empty():
    packed, offsets = utils.pack_group([])
    unpacked = utils.unpack_group(packed)
    assert unpacked == []
    assert offsets == {}


def test_pack_unpack_single():
    entries = [('only', 999999, b'\x00\x01\x02')]
    packed, offsets = utils.pack_group(entries)
    unpacked = utils.unpack_group(packed)
    assert len(unpacked) == 1
    assert unpacked[0] == entries[0]


def test_pack_group_offsets_allow_byte_range_read():
    """Offsets from pack_group can slice the packed bytes to recover each value."""
    entries = [
        ('key1', 1000000, b'value1'),
        ('key2', 2000000, b'value2'),
        ('key3', 3000000, b'a longer value here'),
    ]
    packed, offsets = utils.pack_group(entries)

    assert set(offsets.keys()) == {'key1', 'key2', 'key3'}
    for key, _ts, value in entries:
        offset, length = offsets[key]
        assert length == len(value)
        assert packed[offset:offset + length] == value


def test_pack_group_offsets_merged_range():
    """A single slice spanning multiple keys contains all their values at the right relative offsets."""
    entries = [
        ('a', 100, b'alpha'),
        ('b', 200, b'bravo'),
        ('c', 300, b'charlie'),
    ]
    packed, offsets = utils.pack_group(entries)

    # Simulate a merged byte-range GET (min offset to max offset+length)
    all_offsets = [(k, *offsets[k]) for k in offsets]
    all_offsets.sort(key=lambda x: x[1])
    range_start = all_offsets[0][1]
    last = all_offsets[-1]
    range_end = last[1] + last[2]
    chunk = packed[range_start:range_end]

    for key, _ts, value in entries:
        offset, length = offsets[key]
        rel_offset = offset - range_start
        assert chunk[rel_offset:rel_offset + length] == value


def test_key_to_group_id_single_group():
    """With num_groups=1 every key maps to group 0."""
    for i in range(50):
        assert utils.key_to_group_id(str(i), 1) == 0

def test_is_prime_small():
    assert not utils.is_prime_small(0)
    assert not utils.is_prime_small(1)
    assert utils.is_prime_small(2)
    assert utils.is_prime_small(3)
    assert not utils.is_prime_small(4)
    assert utils.is_prime_small(5)
    assert not utils.is_prime_small(9)
    assert utils.is_prime_small(11)
    assert utils.is_prime_small(97)
    assert not utils.is_prime_small(100)


def test_next_prime():
    assert utils.next_prime(1) == 2
    assert utils.next_prime(2) == 2
    assert utils.next_prime(3) == 3
    assert utils.next_prime(4) == 5
    assert utils.next_prime(10) == 11
    assert utils.next_prime(11) == 11
    assert utils.next_prime(100) == 101


def test_check_local_vs_remote(tmp_path):
    local_path = tmp_path / "local.blt"
    # Create a local booklet file
    with booklet.open(local_path, "n", key_serializer="str", value_serializer="pickle") as db:
        db["key1"] = "value1"
        ts = db.get_timestamp("key1")
    
    # Mock remote_time_bytes
    remote_ts_bytes = booklet.utils.int_to_bytes(ts + 1000, 7) # Future timestamp
    
    with booklet.open(local_path, "w") as db:
        # Remote is newer
        assert utils.check_local_vs_remote(db, remote_ts_bytes, "key1") is True
        
        # Remote is same
        remote_ts_same = booklet.utils.int_to_bytes(ts, 7)
        assert utils.check_local_vs_remote(db, remote_ts_same, "key1") is False
        
        # Remote is older
        remote_ts_older = booklet.utils.int_to_bytes(ts - 1000, 7)
        assert utils.check_local_vs_remote(db, remote_ts_older, "key1") is False
        
        # Key not in remote
        assert utils.check_local_vs_remote(db, None, "key1") is None

def test_create_changelog(tmp_path):
    local_path = tmp_path / "local.blt"
    ri_path = tmp_path / "remote_index.blt"
    
    # Setup local
    with booklet.open(local_path, "n", key_serializer="str", value_serializer="pickle") as db:
        db["new_key"] = "val"
        db["updated_key"] = "new_val"
        db["same_key"] = "same_val"
        
        ts_new = db.get_timestamp("new_key")
        ts_updated = db.get_timestamp("updated_key")
        ts_same = db.get_timestamp("same_key")

    # Setup remote index
    with booklet.FixedLengthValue(ri_path, "n", key_serializer="str", value_len=7) as ri:
        # updated_key is older in remote
        ri["updated_key"] = booklet.utils.int_to_bytes(ts_updated - 1000, 7)
        # same_key is same in remote
        ri["same_key"] = booklet.utils.int_to_bytes(ts_same, 7)
        # new_key is missing in remote

    class MockRemoteSession:
        uuid = "some-uuid"

    with booklet.open(local_path, "w") as db:
        with booklet.FixedLengthValue(ri_path, "r") as ri:
            cl_path, loc_map, comp0 = utils.create_changelog(local_path, db, ri, MockRemoteSession())

            assert cl_path.exists()
            with booklet.FixedLengthValue(cl_path, "r") as cl:
                assert "new_key" in cl
                assert "updated_key" in cl
                assert "same_key" not in cl

            ## The capture side product: every live local key with its
            ## physical (ts, offset, len), valid for compaction_count comp0.
            assert set(loc_map) == {"new_key", "updated_key", "same_key"}
            assert comp0 == db.compaction_count
            with open(local_path, 'rb') as raw:
                for key, (ts, off, ln) in loc_map.items():
                    assert ts == db.get_timestamp(key)
                    _, raw_val = db.get_timestamp(key, include_value=True, decode_value=False)
                    raw.seek(off)
                    assert raw.read(ln) == raw_val

            cl_path.unlink()


# ---------------------------------------------------------------------------
# indirect_copy_remote (cross-credential copy): metadata filter + body + Finding-2
# ---------------------------------------------------------------------------

# A GET response's .metadata as s3func actually builds it: the object's genuine USER
# metadata interleaved with transport fields (some non-string). See
# s3func.utils.add_metadata_from_urllib3 / add_metadata_from_s3_xml.
_MIXED_META = {
    'status': 200,
    'content_length': 7,
    'upload_timestamp': datetime.datetime(2026, 7, 23, tzinfo=datetime.timezone.utc),
    'key': 'srcdb/k1',
    'version_id': 'v-abc',
    'etag': 'e-abc',
    # genuine ebooklet user metadata (all strings):
    'timestamp': '1721000000000000',
    'uuid': 'deadbeefdeadbeefdeadbeefdeadbeef',
    'type': 'Booklet',
    'init_bytes': 'AAAA',
    'format_version': '2',
}
_USER_KEYS = {'timestamp', 'uuid', 'type', 'init_bytes', 'format_version'}


def test_user_metadata_filters_transport():
    out = utils._user_metadata(_MIXED_META)
    assert set(out) == _USER_KEYS
    assert out == {k: _MIXED_META[k] for k in _USER_KEYS}
    # everything kept must satisfy put_object's string contract
    assert all(isinstance(k, str) and isinstance(v, str) for k, v in out.items())
    # empty / None -> {}
    assert utils._user_metadata({}) == {}
    assert utils._user_metadata(None) == {}


class _FakeGetResp:
    def __init__(self, data, metadata):
        self.status = 200
        self.data = data  # non-streaming session: the body lives in .data ...
        self.stream = None  # ... and .stream is None (faithful to stream=False)
        self.metadata = metadata


class _FakeSource:
    def __init__(self, data, metadata):
        self._resp = _FakeGetResp(data, metadata)

    def get_object(self, key):
        return self._resp


class _FakeTarget:
    """Mirrors s3func.put_object's contract: every metadata key/value must be a string."""

    def __init__(self):
        self.calls = []

    def put_object(self, key, body, metadata):
        for mk, mv in metadata.items():
            if not (isinstance(mk, str) and isinstance(mv, str)):
                raise TypeError('metadata keys and values must be strings.')
        self.calls.append((key, body, metadata))

        class _R:
            status = 200

        return _R()


def test_indirect_copy_remote_copies_body_and_user_metadata():
    """Regression on BOTH defects: the transport-contaminated metadata is filtered
    (no TypeError, transport keys stripped) AND the body is non-empty (read from .data,
    not the always-None .stream). Fails two ways on the pre-fix function."""
    src = _FakeSource(b'payload', dict(_MIXED_META))
    tgt = _FakeTarget()
    resp = utils.indirect_copy_remote(src, tgt, 'srcdb/k1', 'tgtdb/k1', 'srcbucket', 'tgtbucket')
    assert resp.status == 200
    assert len(tgt.calls) == 1
    key, body, metadata = tgt.calls[0]
    assert key == 'tgtdb/k1'
    assert body == b'payload'  # non-empty -> Defect B pinned
    assert set(metadata) == _USER_KEYS  # transport fields stripped -> Defect A pinned


def test_indirect_copy_remote_empty_metadata_roundtrips():
    """Grouped-mode child: only transport fields present -> filtered to {}, put_object
    receives an empty dict without raising, body still copied."""
    src = _FakeSource(b'abc', {'status': 200, 'content_length': 3})
    tgt = _FakeTarget()
    utils.indirect_copy_remote(src, tgt, 'a', 'b', 'sb', 'db')
    key, body, metadata = tgt.calls[0]
    assert body == b'abc'
    assert metadata == {}


def test_indirect_copy_remote_refuses_empty_body():
    """A None body (e.g. a future streaming session) must raise, never silently copy empty."""
    src = _FakeSource(None, {'timestamp': '1'})
    tgt = _FakeTarget()
    with pytest.raises(ValueError):
        utils.indirect_copy_remote(src, tgt, 'a', 'b', 'sb', 'db')
    assert tgt.calls == []


# --- end-to-end: the real copy_remote indirect path (different credentials) ---


class _CredS3Session(fake_s3.FakeS3Session):
    """FakeS3Session with configurable credentials, so a copy between two of them
    (different creds) routes copy_remote to the INDIRECT (download->upload) path
    instead of the same-creds server-side copy_object."""

    def __init__(self, store, access_key_id, access_key, **kw):
        super().__init__(store, **kw)
        self._access_key_id = access_key_id
        self._access_key = access_key


class _CredS3Connection(fake_s3.FakeS3Connection):
    def __init__(self, store, db_key, access_key_id, access_key, **kw):
        super().__init__(store, db_key, **kw)
        self._akid = access_key_id
        self._ak = access_key

    def open(self, flag: str = 'r'):
        sess = _CredS3Session(self.store, self._akid, self._ak, bucket=self.bucket)
        if flag == 'r':
            return remote.S3SessionReader(sess, self.db_key, self.threads)
        return remote.S3SessionWriter(sess, sess, self.db_key, self.db_key, self.threads)


def test_copy_remote_indirect_path_different_creds(tmp_path):
    """The one test that exercises the REAL indirect path end-to-end (fake-vs-fake
    same-creds copies never reach it). Different credentials force copy_remote to
    download+upload, then the target db must reopen (db-object metadata preserved
    through the filter) and the per-key child bodies round-trip (read from .data)."""
    store = {}
    src_conn = _CredS3Connection(store, 'srcdb', 'akid-A', 'ak-A')
    with open_ebooklet(src_conn, tmp_path / 'src.blt', flag='n') as eb:
        eb['k1'] = b'v1'
        eb['k2'] = b'v2'
        assert eb.changes().push()

    tgt_conn = _CredS3Connection(store, 'tgtdb', 'akid-B', 'ak-B')  # different creds -> indirect
    with src_conn.open('w') as src:
        result = src.copy_remote(tgt_conn)
    assert not result, f'copy_remote reported failures: {result}'

    fresh = _CredS3Connection(store, 'tgtdb', 'akid-B', 'ak-B')
    with open_ebooklet(fresh, tmp_path / 'tgt.blt', flag='r') as r:
        assert r['k1'] == b'v1'
        assert r['k2'] == b'v2'
