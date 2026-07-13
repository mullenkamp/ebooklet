#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan  5 11:04:13 2023

@author: mike
"""
import logging
import hashlib
import struct
import warnings
import uuid as _uuid
import booklet
import urllib3
from datetime import datetime, timezone
import base64
import portalocker
from concurrent.futures import ThreadPoolExecutor, as_completed
import msgspec

logger = logging.getLogger(__name__)

############################################
### Parameters

int_to_bytes = booklet.utils.int_to_bytes
bytes_to_int = booklet.utils.bytes_to_int
metadata_key_str = booklet.utils.metadata_key_bytes.decode()

## Internal booklet keys (metadata + the reserved slots holding the journal and
## remote-state cache) that user operations must never address directly - a
## set()/del on one of these raw strings would write into the internal slot
## through the ordinary keyed path.
reserved_key_strs = frozenset(
    {metadata_key_str} | {k.decode() for k in booklet.utils.reserved_slot_key_bytes.values()}
)

## The remote storage format version this ebooklet reads and writes. Stamped
## into the db object's S3 metadata on every push; remotes whose stamp exceeds
## it are refused with UnsupportedFormatError (absence of the stamp = 1).
## Format 2 (0.10): generation-named immutable group objects + the manifest/
## metadata/index db-object payload. There is NO legacy read path for format 1
## (deliberate - see the changelog's upgrade recipe).
SUPPORTED_FORMAT_VERSION = 2

## db-object payload layout (format 2). The sections that change together ride
## ONE object - one PUT is the push's atomic commit point:
##   magic(12) | payload_version >H (2) | reserved (2)
##   | manifest_len >Q (8) | meta_len >Q (8) | index_len >Q (8)
##   | manifest: msgspec-JSON {gid: gen13}   (empty dict in per-key mode)
##   | meta:     msgspec-JSON {"timestamp": µs, "data": ...}  (len 0 = absent)
##   | index:    raw FixedLengthValue booklet bytes (value_len=15, unchanged)
## Sections precede the index so the manifest and user metadata are cheap
## ranged GETs. v1 bodies start with booklet's 16-byte fixed-file type uuid,
## so the magic discriminates unambiguously.
DB_MAGIC = b'ebooklet-db\x00'
PAYLOAD_VERSION = 2
PAYLOAD_HEADER_LEN = 40


## The exception classes moved to errors.py in 0.10.0 (typed taxonomy); these
## explicit re-exports keep the old attribute paths
## (utils.UnsupportedFormatError etc.) working for existing callers.
from .errors import (  # noqa: E402
    UnsupportedFormatError as UnsupportedFormatError,
    GroupTooLargeError as GroupTooLargeError,
    ReadOnlyError as ReadOnlyError,
    UUIDMismatchError as UUIDMismatchError,
    RemoteMissingError as RemoteMissingError,
    LockLostError as LockLostError,
    OfflineError as OfflineError,
)


def build_db_payload(manifest, meta_section, index_bytes):
    """
    Assemble the format-2 db-object payload. manifest is {gid_int: gen_str};
    meta_section is the pre-encoded metadata section (or None for absent).
    """
    manifest_bytes = msgspec.json.encode(manifest)
    meta_bytes = meta_section if meta_section is not None else b''
    header = (DB_MAGIC
              + struct.pack('>H', PAYLOAD_VERSION)
              + b'\x00\x00'
              + struct.pack('>Q', len(manifest_bytes))
              + struct.pack('>Q', len(meta_bytes))
              + struct.pack('>Q', len(index_bytes)))
    return header + manifest_bytes + meta_bytes + index_bytes


def parse_db_payload_header(header):
    """
    Validate and split the 40-byte payload header. Returns
    (manifest_len, meta_len, index_len). Raises UnsupportedFormatError for
    anything that is not a payload this version reads (incl. v1 raw-index
    bodies, which start with booklet's type uuid, not the magic).
    """
    if len(header) < PAYLOAD_HEADER_LEN or header[:12] != DB_MAGIC:
        raise UnsupportedFormatError(
            'The remote db object is not a format-2 payload (a format-1 remote, or a '
            'foreign object). 0.10 has no format-1 read path: re-create the remote by '
            "pushing it with flag='n' from an upgraded client."
        )
    payload_version = struct.unpack_from('>H', header, 12)[0]
    if payload_version > PAYLOAD_VERSION:
        raise UnsupportedFormatError(
            f'The remote db object uses payload version {payload_version}, but this '
            f'ebooklet only supports up to {PAYLOAD_VERSION}. Upgrade ebooklet to open it.'
        )
    manifest_len, meta_len, index_len = struct.unpack_from('>QQQ', header, 16)
    return manifest_len, meta_len, index_len


def parse_db_payload(data):
    """
    Split a full format-2 payload into (manifest {gid_int: gen_str},
    meta_section bytes-or-None, index_bytes).
    """
    manifest_len, meta_len, index_len = parse_db_payload_header(data[:PAYLOAD_HEADER_LEN])
    pos = PAYLOAD_HEADER_LEN
    manifest = msgspec.json.decode(data[pos:pos + manifest_len], type=dict[int, str])
    pos += manifest_len
    meta_section = bytes(data[pos:pos + meta_len]) if meta_len else None
    pos += meta_len
    index_bytes = data[pos:pos + index_len]
    return manifest, meta_section, index_bytes


class MetaSection(msgspec.Struct):
    """The decoded metadata section of the db-object payload."""
    timestamp: int
    data: msgspec.Raw = msgspec.Raw(b'null')


def build_meta_section(timestamp_int, data):
    return msgspec.json.encode({'timestamp': timestamp_int, 'data': data})


def parse_meta_section(section):
    """Returns (timestamp_int, decoded_data) or (None, None) for absent."""
    if not section:
        return None, None
    parsed = msgspec.json.decode(section, type=MetaSection)
    return parsed.timestamp, msgspec.json.decode(parsed.data)


def group_obj_key(group_id, gen):
    """The S3 child key of a group object generation (relative to db_key + '/')."""
    return f'{group_id}.{gen}'


def new_generation(current_gen=None):
    """
    A fresh generation token: 13 hex chars (the lock_id idiom). No ordering
    semantics - fsck ages orphans by listing timestamps, never by token.
    Re-rolls on the (~2**-52) collision with the group's current generation:
    the whole point of generations is never overwriting a live object.
    """
    while True:
        gen = _uuid.uuid4().hex[:13]
        if gen != current_gen:
            return gen

############################################
### Group functions


def is_prime_small(n: int) -> bool:
    """Trial-division primality test suitable for small n."""
    if n < 2:
        return False
    if n < 4:
        return True
    if n % 2 == 0 or n % 3 == 0:
        return False
    i = 5
    while i * i <= n:
        if n % i == 0 or n % (i + 2) == 0:
            return False
        i += 6
    return True


def next_prime(n: int) -> int:
    """Return the smallest prime >= n."""
    if n <= 2:
        return 2
    candidate = n if n % 2 != 0 else n + 1
    while not is_prime_small(candidate):
        candidate += 2
    return candidate


def key_to_group_id(key: str, num_groups: int) -> int:
    digest = hashlib.blake2b(key.encode(), digest_size=4).digest()
    return int.from_bytes(digest, 'big') % num_groups


_MAX_GROUP_BYTES = 2**32 - 1   # the >I offset and length fields' ceiling


def pack_group(entries: list[tuple[str, int, bytes]]) -> tuple[bytes, dict[str, tuple[int, int]]]:
    buf = struct.pack('>I', len(entries))
    offsets = {}
    pos = 4
    for key, timestamp, value in entries:
        key_bytes = key.encode()
        ## F7 guard: check BEFORE appending, so an oversized group aborts
        ## without materializing a >4 GiB buffer (offsets and value lengths
        ## are 4-byte fields - overflowing them would corrupt the index).
        entry_size = 2 + len(key_bytes) + 7 + 4 + len(value)
        if pos + entry_size > _MAX_GROUP_BYTES:
            raise GroupTooLargeError(
                f'packing this group would exceed {_MAX_GROUP_BYTES} bytes (the 4-byte '
                'offset/length ceiling). Not retryable as-is: re-create the database '
                "with a larger num_groups (flag='n'), or store smaller values."
            )
        buf += struct.pack('>H', len(key_bytes))
        pos += 2
        buf += key_bytes
        pos += len(key_bytes)
        buf += int_to_bytes(timestamp, 7)
        pos += 7
        buf += struct.pack('>I', len(value))
        pos += 4
        offsets[key] = (pos, len(value))
        buf += value
        pos += len(value)
    return buf, offsets


def unpack_group(data: bytes) -> list[tuple[str, int, bytes]]:
    offset = 0
    num_entries = struct.unpack_from('>I', data, offset)[0]
    offset += 4
    entries = []
    for _ in range(num_entries):
        key_len = struct.unpack_from('>H', data, offset)[0]
        offset += 2
        key = data[offset:offset + key_len].decode()
        offset += key_len
        timestamp = bytes_to_int(data[offset:offset + 7])
        offset += 7
        value_len = struct.unpack_from('>I', data, offset)[0]
        offset += 4
        value = data[offset:offset + value_len]
        offset += value_len
        entries.append((key, timestamp, value))
    return entries


############################################
### Functions


def s3session_finalizer(session):
    """
    The finalizer function for S3Remote instances.
    """
    session._session.clear()


def ebooklet_finalizer(local_file, remote_index, remote_session, lock, journal=None):
    """
    The finalizer function for book instances. Persists the journal BEFORE the
    local file closes, so a session torn down by garbage collection (close()
    never called) still records its pending state; a finalizer must never
    raise, so persistence failures are logged and swallowed.
    """
    if journal is not None:
        try:
            journal.persist(local_file)
        except Exception:
            logger.exception('journal persistence failed during finalization; pending state may be stale')
    local_file.close()
    remote_index.close()
    remote_session.close()
    if lock is not None:
        lock.release()


def open_remote_conn(remote_conn, flag, local_file_exists):
    """

    """
    remote_session = remote_conn.open(flag)

    if flag == 'r' and not remote_session.initialized and not local_file_exists:
        raise RemoteMissingError('No file was found in the remote, but the local file was open for read without creating a new file.')

    ebooklet_type = remote_session.type

    return remote_session, ebooklet_type


def check_local_remote_sync(local_file, remote_session, flag):
    """

    """
    overwrite_remote_index = False

    remote_uuid = remote_session.uuid
    if remote_uuid and flag != 'n':
        local_uuid = local_file.uuid

        if remote_uuid != local_uuid:
            raise UUIDMismatchError('The local file has a different UUID than the remote. Use a different local file path or delete the existing one.')

        ## Check timestamp to determine if the local remote_index needs to be updated
        if (remote_session.timestamp > local_file._file_timestamp):
            overwrite_remote_index = True

    return overwrite_remote_index


def init_local_file(local_file_path, flag, remote_session, value_serializer, n_buckets, buffer_size):
    """

    """
    remote_uuid = remote_session.uuid

    if local_file_path.exists():

        if flag == 'n':
            if remote_uuid:
                local_file = booklet.open(local_file_path, flag='n', init_bytes=remote_session._init_bytes)
                local_file._n_keys = 0
            else:
                local_file = booklet.open(local_file_path, flag='n', key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)

            overwrite_remote_index = True
        else:
            ## The local file will need to always be open for write since data will be loaded from the remote regardless if the user has only opened it for read-only
            local_file = booklet.open(local_file_path, 'w')

            overwrite_remote_index = check_local_remote_sync(local_file, remote_session, flag)

    else:
        if remote_uuid:
            ## Init with the remote bytes - keeps the remote uuid and timestamp
            local_file = booklet.open(local_file_path, flag='n', init_bytes=remote_session._init_bytes)
            local_file._n_keys = 0

            overwrite_remote_index = True
        else:
            ## Else create a new file
            local_file = booklet.open(local_file_path, flag='n', key_serializer='str', value_serializer=value_serializer, n_buckets=n_buckets, buffer_size=buffer_size)

            overwrite_remote_index = True

    return local_file, overwrite_remote_index


def get_remote_index_file(local_file_path, overwrite_remote_index, remote_session, flag):
    """
    Ensure the local remote-index sidecar file exists (fetching + parsing the
    db-object payload when needed). Returns (remote_index_path, manifest,
    meta_section) - manifest/meta_section are None when nothing was fetched
    (the caller falls back to the persisted remote-state slot).
    """
    remote_index_path = local_file_path.parent.joinpath(local_file_path.name + '.remote_index')

    manifest = None
    meta_section = None
    if not remote_index_path.exists() or overwrite_remote_index:
        fetched, manifest, meta_section = fetch_remote_index(remote_index_path, remote_session)
        if not fetched:
            manifest = None
            meta_section = None

    return remote_index_path, manifest, meta_section


def fetch_remote_index(dest_path, remote_session):
    """
    Download the remote db object, parse the format-2 payload, and write the
    INDEX section to dest_path. Returns (True, manifest, meta_section) on
    success, (False, None, None) when there is nothing to fetch (no remote,
    or a 404); raises UnsupportedFormatError on a non-format-2 body and
    HTTPError on other failures.
    """
    if not remote_session.initialized:
        return False, None, None

    index0 = remote_session.get_object()
    if index0.status in (200, 206):
        manifest, meta_section, index_bytes = parse_db_payload(index0.data)
        with portalocker.Lock(dest_path, 'wb', timeout=120) as f:
            f.write(index_bytes)
        return True, manifest, meta_section
    elif index0.status == 404:
        return False, None, None
    else:
        raise urllib3.exceptions.HTTPError(index0.error)


def fetch_remote_state(remote_session):
    """
    Cheap refresh of the manifest + metadata section only: two ranged GETs of
    the db object (header, then the pre-index sections) - the index body is
    not downloaded. Returns (manifest, meta_section) or (None, None) when the
    remote is absent.
    """
    if not remote_session.initialized:
        return None, None

    head = remote_session.get_object(range_start=0, range_end=PAYLOAD_HEADER_LEN - 1)
    if head.status == 404:
        return None, None
    if head.status not in (200, 206):
        raise urllib3.exceptions.HTTPError(head.error)
    manifest_len, meta_len, _ = parse_db_payload_header(head.data[:PAYLOAD_HEADER_LEN])

    manifest = {}
    meta_section = None
    if manifest_len or meta_len:
        sections = remote_session.get_object(
            range_start=PAYLOAD_HEADER_LEN,
            range_end=PAYLOAD_HEADER_LEN + manifest_len + meta_len - 1)
        if sections.status not in (200, 206):
            raise urllib3.exceptions.HTTPError(sections.error)
        data = sections.data
        manifest = msgspec.json.decode(data[:manifest_len], type=dict[int, str]) if manifest_len else {}
        meta_section = bytes(data[manifest_len:manifest_len + meta_len]) if meta_len else None
    return manifest, meta_section


def refresh_local_metadata(local_file, journal, meta_section):
    """
    Refresh the local metadata slot from a fetched payload's metadata section:
    only when the remote's is strictly newer, and NEVER over an unpushed local
    edit (journal.meta_pending) - the metadata read-your-writes gate.
    """
    if journal.meta_pending or not meta_section:
        return
    remote_ts, data = parse_meta_section(meta_section)
    local_ts = local_file.get_timestamp(booklet.utils.metadata_key_bytes.decode())
    if local_ts is None or (remote_ts is not None and remote_ts > local_ts):
        local_file.set_metadata(data, timestamp=remote_ts)


def open_remote_index(remote_index_path, flag, n_buckets, buffer_size):
    """
    Open the local remote-index booklet (writable in every session mode - see
    the in-function note; fresh file when none exists).

    Index entry layout (fixed value_len=15): timestamp(7) + offset(4) + length(4).
    Per-key mode: offset and length are always 0. Grouped mode: offset/length
    locate the member value inside its group object; length is the value's byte
    length and MAY be 0 (an empty value) - it is NOT a mode discriminator. Future
    layouts change the fixed value_len (the layout discriminator), gated by the
    db object's format_version metadata.
    """
    if remote_index_path.exists():
        ## Writable in EVERY session mode since 0.10: replay-on-swap re-applies
        ## journaled deletes to each fresh index copy (open and re-pull), and
        ## 'r' sessions carry journals too. Safe: the local file is held under
        ## an exclusive portalocker lock in every mode, so no second process
        ## ever shares this sidecar. (Trade-off: 'r' sidecars lose the mmap
        ## read path.)
        return booklet.FixedLengthValue(remote_index_path, 'w')
    else:
        return booklet.FixedLengthValue(remote_index_path, 'n', key_serializer='str', value_len=15, n_buckets=n_buckets, buffer_size=buffer_size)


class MissingRemoteObject:
    """
    Truthy failure marker: the remote index claims key(s) whose backing S3
    object returned 404 - or, for grouped members, whose group object exists
    (200) but no longer contains them (object_exists=True). Every value fetch
    is gated on the index claiming the key (check_local_vs_remote), so either
    state means the store contradicts its own index - a legitimately-deleted
    key seen through a stale reader index, or a real integrity fault. main.py's
    re-check protocol (_resolve_missing) disambiguates the two by re-pulling
    the index.

    Deliberately truthy (no __bool__/__len__) and non-None so it passes both
    failure gates: the point-read `if failure:` and load_items' `is not None`.
    """
    __slots__ = ('s3_key', 'keys', 'object_exists')

    def __init__(self, s3_key: str, keys: list, object_exists: bool = False):
        self.s3_key = s3_key   # the S3 object key (group id str, per-key key, or '_metadata')
        self.keys = keys       # the ebooklet keys the index claims live in that object
        self.object_exists = object_exists  # True: object present but lacks these members

    def __repr__(self):
        if self.object_exists:
            return (f"remote group object '{self.s3_key}' no longer contains member key(s) "
                    f"{self.keys} but the remote index still claims them (integrity failure); "
                    f"delete these keys and re-push, or restore the members")
        return (f"remote object '{self.s3_key}' is missing but the remote index still claims "
                f"key(s) {self.keys} (integrity failure); delete these keys and re-push, "
                f"or restore the object")


def get_remote_value(local_file, key, remote_session):
    """
    Fetch one per-key-mode value. (User metadata is no longer a separate
    object in format 2 - it rides the db-object payload.)
    """
    resp = remote_session.get_object(key)

    if resp.status == 200:
        timestamp = int(resp.metadata['timestamp'])
        local_file.set(key, resp.data, timestamp, encode_value=False)
    elif resp.status == 404:
        ## This fetch only runs when the remote index claims the key, so a 404
        ## is never legitimate absence - surface it for the re-check protocol.
        return MissingRemoteObject(key, [key])
    else:
        return resp.error

    return None


def upload_group(group_id, gen, local_file, remote_session, keys_in_group):
    """
    Pack and PUT one group's members to a FRESH generation object - never
    overwriting the live generation (immutability is the format-2 invariant).
    The caller skips emptied groups entirely (no PUT; the group leaves the
    manifest at commit and its old generation is GC'd after). Worker contract:
    return errors, never raise (a raise would crash future.result()).
    """
    entries = []
    for key in keys_in_group:
        result = local_file.get_timestamp(key, include_value=True, decode_value=False)
        if result is None:
            continue
        time_int_us, valb = result
        entries.append((key, time_int_us, valb))

    try:
        packed, offsets = pack_group(entries)
    except GroupTooLargeError as err:
        return err, None
    resp = remote_session.put_object(group_obj_key(group_id, gen), packed)
    if resp.status // 100 != 2:
        return resp.error, None
    return None, offsets


## Per-entry header layout inside a packed group object (see pack_group):
## [key_len: >H][key][timestamp: 7 bytes][value_len: >I][value]
group_entry_fixed_overhead = 2 + 7 + 4


def recover_group_members(group_id, gen, key_infos, local_file, remote_session, report_missing_members=True):
    """
    Fallback for when ranged reads into a group object cannot be verified against the
    remote_index offsets (a corrupted or shifted group object). Downloads the whole
    self-describing group object and recovers whichever requested members it actually
    contains, trusting the object's own embedded keys/timestamps rather than the index.

    Members the index claims but the (200) object does not contain are reported as a
    MissingRemoteObject marker so read paths route them through the re-check protocol
    (_resolve_missing) - previously `key in db` said True while `db[key]` silently
    raised KeyError. The push path passes report_missing_members=False: there the
    absent-member state is deliberately self-healed (the lost-keys drop in
    update_remote), which is also the recovery mechanism for the read-side error.
    """
    obj_key = group_obj_key(group_id, gen)
    resp = remote_session.get_object(obj_key)
    if resp.status == 200:
        entries = {key: (timestamp, value) for key, timestamp, value in unpack_group(resp.data)}
        missing = []
        for key, offset, length, timestamp_int in key_infos:
            entry = entries.get(key)
            if entry is not None:
                timestamp, value = entry
                local_file.set(key, value, timestamp, encode_value=False)
            else:
                missing.append(key)
        if report_missing_members and missing:
            ## Recovered members above stay materialized - they are valid remote
            ## data. The marker carries only the still-missing subset. An empty
            ## missing list must return None (plain success), never a truthy
            ## empty marker that would trigger a needless index re-pull.
            return MissingRemoteObject(obj_key, missing, object_exists=True)
    elif resp.status == 404:
        ## key_infos comes from remote-index entries, so the index claims these
        ## members - a missing group object is never legitimate absence here.
        return MissingRemoteObject(obj_key, [k for k, _o, _l, _t in key_infos])
    else:
        return resp.error
    return None


def get_remote_group_values(group_id, gen, key_infos, local_file, remote_session, report_missing_members=True):
    """
    key_infos: list of (key, offset, length, timestamp_int)

    Each member read is verified against the group object's embedded entry header
    (the key itself and the value length precede every value in the packed layout)
    before being trusted - a stale index offset would otherwise silently deliver
    another entry's bytes. On any verification failure the whole (self-describing)
    group object is downloaded and parsed instead (recover_group_members;
    report_missing_members is passed through - see its docstring).
    """
    sorted_infos = sorted(key_infos, key=lambda x: x[1])

    ## Start the ranged read at the first member's entry header so every requested
    ## member's header is inside the fetched range.
    first_key, first_offset, _, _ = sorted_infos[0]
    range_start = first_offset - group_entry_fixed_overhead - len(first_key.encode())
    last = sorted_infos[-1]
    range_end = last[1] + last[2] - 1

    if range_start < 0:
        logger.warning(f"Group {group_id}: stored index offsets are malformed; recovering members from the full group object.")
        return recover_group_members(group_id, gen, key_infos, local_file, remote_session, report_missing_members)

    resp = remote_session.get_object(group_obj_key(group_id, gen), range_start=range_start, range_end=range_end)
    if resp.status in (200, 206):
        data = resp.data
        verified = []
        for key, offset, length, timestamp_int in sorted_infos:
            key_bytes = key.encode()
            header_start = offset - group_entry_fixed_overhead - len(key_bytes) - range_start
            rel_offset = offset - range_start

            ok = header_start >= 0 and rel_offset + length <= len(data)
            if ok:
                key_len = struct.unpack_from('>H', data, header_start)[0]
                ok = (key_len == len(key_bytes)
                      and data[header_start + 2:header_start + 2 + key_len] == key_bytes
                      and struct.unpack_from('>I', data, header_start + 2 + key_len + 7)[0] == length)
            if not ok:
                verified = None
                break
            verified.append((key, data[rel_offset:rel_offset + length], timestamp_int))

        if verified is None:
            logger.warning(f"Group {group_id}: stored index offsets do not match the group object layout (corrupted or shifted object); recovering members from the full group object.")
            return recover_group_members(group_id, gen, key_infos, local_file, remote_session, report_missing_members)

        for key, value, timestamp_int in verified:
            local_file.set(key, value, timestamp_int, encode_value=False)
    elif resp.status == 404:
        ## sorted_infos comes from remote-index entries, so the index claims
        ## these members - a missing group object is never legitimate absence.
        return MissingRemoteObject(group_obj_key(group_id, gen), [k for k, _o, _l, _t in key_infos])
    elif resp.status == 416:
        ## Requested range extends past the object's end - the object shrank
        ## relative to the index. Recover whatever actually exists.
        logger.warning(f"Group {group_id}: stored index offsets extend past the group object; recovering members from the full group object.")
        return recover_group_members(group_id, gen, key_infos, local_file, remote_session, report_missing_members)
    else:
        return resp.error
    return None


def get_remote_group_value(group_id, gen, key, offset, length, timestamp_int, local_file, remote_session):
    return get_remote_group_values(group_id, gen, [(key, offset, length, timestamp_int)], local_file, remote_session)


def check_local_vs_remote(local_file, remote_time_bytes, key):
    """

    """
    if remote_time_bytes is None:
        return None

    remote_time_int = bytes_to_int(remote_time_bytes)
    local_time_int = local_file.get_timestamp(key)

    if local_time_int:
        if remote_time_int <= local_time_int:
            return False

    return True


#################################################
### local/remote changelog


def create_changelog(local_file_path, local_file, remote_index, remote_session, journal=None):
    """
    Build the push changelog: the UNION of the timestamp diff (local newer than
    remote index) and the journal's pending writes. The timestamp diff catches
    writes whose journal entry was lost in a crash window (booklet auto-flushes
    data ahead of the sync-boundary journal persistence); the journal catches
    clock-skewed edits the diff can never see, and is the only source of
    deletes (which live outside the changelog file entirely).

    Skew normalization: a journaled key whose local timestamp does not beat the
    remote entry's is bumped to max(now, remote+1) BEFORE it enters the
    changelog - without this the edit would push with its older stamp and
    readers that already materialized the newer remote value would never pull
    it (the 0.8.4 clock-skew case, now closed instead of warned about).
    """
    changelog_path = local_file_path.parent.joinpath(local_file_path.name + '.changelog')
    if remote_index is not None:
        n_buckets = remote_index._n_buckets
    else:
        n_buckets = local_file._n_buckets

    journal_written = journal.written if journal is not None else ()

    with booklet.FixedLengthValue(changelog_path, 'n', key_serializer='str', value_len=14, n_buckets=n_buckets) as f:
        if remote_session.uuid and remote_index is not None:
            for key, local_int_us in local_file.timestamps():
                remote_val = remote_index.get(key)
                if remote_val:
                    remote_ts_bytes = remote_val[:7]
                    remote_int_us = bytes_to_int(remote_ts_bytes)
                    if local_int_us > remote_int_us:
                        local_bytes_us = int_to_bytes(local_int_us, 7)
                        f[key] = local_bytes_us + remote_ts_bytes
                else:
                    local_bytes_us = int_to_bytes(local_int_us, 7)
                    f[key] = local_bytes_us + int_to_bytes(0, 7)

            ## Journal union: pending writes the timestamp diff missed. The
            ## only in-range candidates are skew-stamped edits (local <= remote)
            ## and journal entries whose local value no longer exists.
            now_int_us = booklet.utils.make_timestamp_int()
            for key in sorted(journal_written):
                if key in f:
                    continue
                local_int_us = local_file.get_timestamp(key)
                if local_int_us is None:
                    warnings.warn(
                        f"The journal records an unpushed write for '{key}' but the key has no "
                        'local value (evicted or externally removed) - it is DROPPED from this '
                        'push. If the value mattered, re-set it before pushing.',
                        UserWarning, stacklevel=3,
                    )
                    journal.discard_written(key)
                    continue
                remote_val = remote_index.get(key)
                if remote_val:
                    remote_int_us = bytes_to_int(remote_val[:7])
                    new_ts = max(now_int_us, remote_int_us + 1)
                    local_file.set_timestamp(key, new_ts)
                    warnings.warn(
                        f"The unpushed local edit of '{key}' carried a timestamp at or before the "
                        "remote's (clock skew or an explicit set_timestamp); its timestamp was "
                        'advanced so the edit propagates to readers that already hold the remote value.',
                        UserWarning, stacklevel=3,
                    )
                    f[key] = int_to_bytes(new_ts, 7) + remote_val[:7]
                else:
                    f[key] = int_to_bytes(local_int_us, 7) + int_to_bytes(0, 7)

            ## (User metadata no longer rides the changelog: in format 2 it is
            ## embedded in the db-object payload, driven by journal.meta_pending.)
        else:
            for key, local_int_us in local_file.timestamps():
                local_bytes_us = int_to_bytes(local_int_us, 7)
                f[key] = local_bytes_us + int_to_bytes(0, 7)

            ## Journal union, no-remote branch: every local key is already in
            ## the changelog, so only the missing-local-value warning applies.
            for key in sorted(journal_written):
                if key not in f:
                    warnings.warn(
                        f"The journal records an unpushed write for '{key}' but the key has no "
                        'local value (evicted or externally removed) - it is DROPPED from this '
                        'push. If the value mattered, re-set it before pushing.',
                        UserWarning, stacklevel=3,
                    )
                    journal.discard_written(key)

    return changelog_path


def view_changelog(changelog_path):
    """

    """
    with booklet.FixedLengthValue(changelog_path) as f:
        for key, val in f.items():
            local_bytes_us = val[:7]
            remote_bytes_us = val[7:]
            local_int_us = bytes_to_int(local_bytes_us)
            remote_int_us = bytes_to_int(remote_bytes_us)
            if remote_int_us == 0:
                remote_ts = None
            else:
                remote_ts = datetime.fromtimestamp(remote_int_us*0.000001, tz=timezone.utc)

            dict1 = {
                'key': key,
                'remote_timestamp': remote_ts,
                'local_timestamp': datetime.fromtimestamp(local_int_us*0.000001, tz=timezone.utc)
                }

            yield dict1


##############################################
### Update remote


def upload_value(key, local_file, remote_session):
    """
    Upload one per-key-mode value. (User metadata is never uploaded as an
    object in format 2 - it rides the db-object payload.)
    """
    time_int_us, valb = local_file.get_timestamp(key, include_value=True, decode_value=False)
    resp = remote_session.put_object(key, valb, {'timestamp': str(time_int_us)})
    if resp.status // 100 != 2:
        return resp.error
    else:
        return None


def _build_meta_section_for_push(local_file, journal, remote_state, replace_pending, time_int_us):
    """
    Decision 9 (review-amended): the metadata section a push embeds.
    - meta_pending: the user edited metadata locally - embed the local slot,
      normalizing its timestamp past the remote's (a skew-stamped edit must
      not lose a timestamp comparison; mirrors the key skew rule).
    - replacement: never carry the OLD remote's metadata into the replacement
      ('n' semantics promise it is gone) - local slot or nothing.
    - otherwise: carry the cached remote section forward verbatim (embedding
      the local slot unconditionally would wipe remote metadata on the first
      push from a fresh local file - init_bytes carries no metadata block).
    """
    if journal.meta_pending:
        out = local_file.get_metadata(include_timestamp=True)
        if out is None:
            data, local_ts = None, None
        else:
            data, local_ts = out
        remote_meta_ts, _ = parse_meta_section(remote_state.meta_section)
        ts = local_ts if local_ts is not None else time_int_us
        if remote_meta_ts is not None and ts <= remote_meta_ts:
            ts = max(time_int_us, remote_meta_ts + 1)
            local_file.set_metadata(data, timestamp=ts)
            warnings.warn(
                'The unpushed local metadata edit carried a timestamp at or before the '
                "remote's; its timestamp was advanced so the edit propagates to readers.",
                UserWarning, stacklevel=4,
            )
        return build_meta_section(ts, data)
    if replace_pending:
        ## Never carry the OLD remote's metadata into a replacement - not even
        ## via the local slot (the open transparently pulls it there for
        ## pre-push reads; only a user edit, i.e. meta_pending above, counts).
        return None
    return remote_state.meta_section


def update_remote(local_file, remote_index, remote_index_path, changelog_path, remote_session, force_push, journal, remote_state, replace_pending, ebooklet_type, num_groups=None, lock=None):
    """
    Push the changelog to the remote - the format-2 protocol:

      A. changelog union -> affected groups -> pull unmaterialized members
         from the OLD generations (read-your-writes gated).
      B. PUT each repacked group to a FRESH generation object (immutable -
         readers on the old manifest are untouched). Emptied groups PUT
         nothing. New index entries are STAGED, not written to the live
         sidecar - the sidecar never references uncommitted generations.
      C. Commit: ONE db-object PUT carrying the new manifest, the metadata
         section, and the staged index bytes. lock.verify() immediately
         before - the point of no return. Only after success: the staged
         entries apply to the live sidecar, the journal clears (for exactly
         the committed state), and the remote-state cache persists.
      D. GC: exact-key deletes of the replaced/emptied OLD generations.
         Failures are log-only orphans (nothing references them; fsck
         sweeps). A REPLACEMENT push then sweeps everything under the
         db namespace not in the new manifest (after re-verifying the lock)
         - the old remote stays fully intact unless the commit landed.

    A crash or failure before C leaves readers on the old, fully-consistent
    state and this session fully retryable; between C and D leaves only
    invisible orphans.
    """
    pre_push_manifest = dict(remote_state.manifest)
    deletes = journal.deletes   # read here; journal mutated only post-commit

    ## Upload data and update the remote_index file
    updated = False
    failures = {}
    failed_gids = set()
    staged_entries = {}
    new_gens = {}
    emptied_gids = set()
    staged_index_bytes = None

    with booklet.FixedLengthValue(changelog_path) as cl:
        if num_groups is not None:
            ## Grouped upload path
            affected_group_ids = set()

            for key in cl:
                affected_group_ids.add(key_to_group_id(key, num_groups))

            ## Also include groups affected by deletes
            for key in deletes:
                affected_group_ids.add(key_to_group_id(key, num_groups))

            ## Build FULL key lists per affected group: the union of locally-present
            ## keys and remote-index keys. A group object is completely replaced on
            ## upload, so every current member must be packed - not just the keys
            ## that happen to be materialized in the local file.
            group_key_sets = {gid: set() for gid in affected_group_ids}
            for key in local_file.keys():
                if key == metadata_key_str:
                    continue
                gid = key_to_group_id(key, num_groups)
                if gid in group_key_sets:
                    group_key_sets[gid].add(key)

            ## Members whose local value is missing or older than the remote must be
            ## pulled down before their group can be repacked (one ranged read per group).
            groups_to_download = {}
            pull_count = 0
            pull_bytes = 0
            ## NOTE: iterate items() in a single pass - one scan instead of a
            ## per-key chain lookup for every get(). (Before booklet 0.12.6 this
            ## was also mandatory: iterators held the thread lock across yields,
            ## so get() during iteration self-deadlocked. That constraint is
            ## gone, but the single pass remains the right access pattern.)
            for key, remote_val in remote_index.items():
                if key == metadata_key_str or key in deletes:
                    continue
                gid = key_to_group_id(key, num_groups)
                if gid not in group_key_sets:
                    continue
                group_key_sets[gid].add(key)
                ## Read-your-writes gate: a journaled pending write is the
                ## truth for its key - never pull the remote value over it
                ## (this also covers the length==0 empty-value branch below).
                ## Skew-stamped journaled edits were already timestamp-
                ## normalized by create_changelog, so this gate is belt.
                if key in journal.written:
                    continue
                if remote_val and check_local_vs_remote(local_file, remote_val[:7], key):
                    if local_file.get_timestamp(key) is not None:
                        logger.warning(f"Push is replacing the locally-stored value of '{key}' with the newer remote value before repacking its group - any unpushed local modification to it is discarded (its local timestamp is older than the remote's).")
                    offset = bytes_to_int(remote_val[7:11])
                    length = bytes_to_int(remote_val[11:15])
                    timestamp_int = bytes_to_int(remote_val[:7])
                    if length > 0:
                        groups_to_download.setdefault(gid, []).append((key, offset, length, timestamp_int))
                        pull_count += 1
                        pull_bytes += length
                    else:
                        ## A grouped member with length 0 is an EMPTY value, not
                        ## a per-key entry (those never reach this loop). There
                        ## is nothing to download - the value IS b''. Materialize
                        ## it directly, otherwise a locally-absent empty member
                        ## would fall into the lost-keys drop below and be
                        ## silently deleted by the repack.
                        local_file.set(key, b'', timestamp_int, encode_value=False)

            if groups_to_download:
                logger.info(f"Pulling {pull_count} group member value(s) (~{pull_bytes} bytes) from {len(groups_to_download)} group(s) so the groups can be repacked in full.")
                with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
                    ## report_missing_members=False: on the push path an absent
                    ## member is deliberately self-healed by the lost-keys drop
                    ## below - a loud marker here would make the push fail
                    ## permanently instead of repairing the group.
                    pull_futures = {}
                    for gid, key_infos in groups_to_download.items():
                        old_gen = pre_push_manifest.get(gid)
                        if old_gen is None:
                            ## The index claims members of a group the manifest
                            ## does not reference - an integrity fault; the
                            ## group cannot be repacked in full.
                            failures[gid] = MissingRemoteObject(
                                f'{gid}.<unmanifested>', [k for k, _o, _l, _t in key_infos])
                            group_key_sets.pop(gid, None)
                            continue
                        pull_futures[executor.submit(get_remote_group_values, gid, old_gen, key_infos, local_file, remote_session, False)] = gid
                    for future in as_completed(pull_futures):
                        gid = pull_futures[future]
                        error = future.result()
                        if error is not None:
                            ## Never upload a partially-materialized group - leave the
                            ## old remote group object intact and report the failure.
                            failures[gid] = error
                            group_key_sets.pop(gid, None)

            ## Any member that still has no local value lost its remote bytes at some
            ## earlier point (e.g. a partial push from a version < 0.8.4). Repack the
            ## group without it and remove the dangling index entry.
            group_keys = {}
            for gid, keys_set in group_key_sets.items():
                keys_in_group = []
                lost_keys = []
                for key in keys_set:
                    if local_file.get_timestamp(key) is None:
                        lost_keys.append(key)
                    else:
                        keys_in_group.append(key)
                if lost_keys:
                    logger.warning(f"Group {gid}: dropping {len(lost_keys)} key(s) whose remote bytes no longer exist (likely a partial push from a version < 0.8.4): {sorted(lost_keys)}")
                    for key in lost_keys:
                        if key in remote_index:
                            del remote_index[key]
                    updated = True
                group_keys[gid] = keys_in_group

            ## Phase B: PUT each repacked group to a FRESH generation. Emptied
            ## groups PUT nothing - they leave the manifest at commit and
            ## their old generation is GC'd in phase D. New index entries are
            ## STAGED (applied to the live sidecar only after the commit).
            with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
                futures = {}
                for gid, keys_in_group in group_keys.items():
                    if not keys_in_group:
                        emptied_gids.add(gid)
                        updated = True
                        continue
                    gen = new_generation(pre_push_manifest.get(gid))
                    new_gens[gid] = gen
                    f = executor.submit(upload_group, gid, gen, local_file, remote_session, keys_in_group)
                    futures[f] = gid

                for future in as_completed(futures):
                    gid = futures[future]
                    error, offsets = future.result()
                    if error is None:
                        for key in group_keys[gid]:
                            if key in offsets:
                                ts = local_file.get_timestamp(key)
                                if ts:
                                    offset, length = offsets[key]
                                    staged_entries[key] = int_to_bytes(ts, 7) + int_to_bytes(offset, 4) + int_to_bytes(length, 4)
                        updated = True
                    else:
                        failures[gid] = error
                        new_gens.pop(gid, None)   # abandoned PUT (if any) = invisible orphan

            failed_gids = {gid for gid in failures if isinstance(gid, int)}

        else:
            ## Per-key upload path (legacy). Objects are overwritten in place
            ## (each PUT is object-atomic; no cross-key snapshot isolation -
            ## documented), and index entries stay write-through.
            with ThreadPoolExecutor(max_workers=remote_session.threads) as executor:
                futures = {}
                for key in cl:
                    f = executor.submit(upload_value, key, local_file, remote_session)
                    futures[f] = key

                for future in as_completed(futures):
                    key = futures[future]
                    run_result = future.result()
                    if run_result is None:
                        remote_index[key] = cl[key][:7] + b'\x00' * 8
                        updated = True
                    else:
                        failures[key] = run_result

    if failures:
        non_retryable = [k for k, v in failures.items() if isinstance(v, GroupTooLargeError)]
        if non_retryable:
            logger.warning(f"There were {len(failures)} items that failed to upload; group(s) {non_retryable} exceed the 4 GiB pack limit and will NOT succeed on a plain retry (re-shard with a larger num_groups).")
        else:
            logger.warning(f"There were {len(failures)} items that failed to upload. Please run this again.")

    ## Flush the live sidecar (still WITHOUT the staged entries in grouped
    ## mode - it must never reference uncommitted generations).
    remote_index.sync()

    ## Build the index bytes the commit will carry: grouped mode applies the
    ## staged entries and committed delete-entry removals to a throwaway COPY
    ## of the sidecar; per-key mode reads the live sidecar as-is.
    committed_delete_keys = []
    if num_groups is not None:
        committed_delete_keys = [k for k in deletes if key_to_group_id(k, num_groups) not in failed_gids]
        staged_path = remote_index_path.parent.joinpath(remote_index_path.name + '.staged')
        with remote_index._thread_lock:
            remote_index._file.seek(0)
            base_index_bytes = remote_index._file.read()
        try:
            with open(staged_path, 'wb') as f:
                f.write(base_index_bytes)
            staged = booklet.FixedLengthValue(staged_path, 'w')
            try:
                for key, entry in staged_entries.items():
                    staged[key] = entry
                for key in committed_delete_keys:
                    if key in staged:
                        del staged[key]
                staged.sync()
                with staged._thread_lock:
                    staged._file.seek(0)
                    staged_index_bytes = staged._file.read()
            finally:
                staged.close()
        finally:
            try:
                staged_path.unlink()
            except FileNotFoundError:
                pass

    ## Phase C - the commit. Also runs for a metadata-only push (metadata no
    ## longer rides the changelog - it is embedded at commit), when the remote
    ## does not exist yet, or when a replacement is pending, so those cases
    ## still materialize instead of being silent no-ops.
    if updated or force_push or journal.meta_pending or (deletes and num_groups is None) or replace_pending or not remote_session.initialized:
        ## A partially-failed REPLACEMENT push commits NOTHING: the old remote
        ## stays fully intact (readable, uncorrupted) and the retry redoes the
        ## whole replacement. (The pre-0.10 wipe-first protocol left a wiped,
        ## half-uploaded remote here.)
        if replace_pending and failures:
            logger.warning('The replacement push had upload failures - NOT committing; the existing remote is untouched. Re-run the push.')
            return failures

        time_int_us = booklet.utils.make_timestamp_int()

        ## Get main file init bytes. Direct _file access moves the shared file
        ## position, so hold the owning booklet's thread lock (booklet's own
        ## contract since 0.12.6: every position-mover locks).
        local_file._set_file_timestamp(time_int_us)
        with local_file._thread_lock:
            local_file._file.seek(0)
            local_init_bytes = bytearray(local_file._file.read(200))
        if local_init_bytes[:16] != booklet.utils.uuid_variable_blt:
            raise ValueError(
                'The local file does not start with the variable-length booklet magic - '
                f'not an ebooklet local file (first bytes: {bytes(local_init_bytes[:16])!r}).'
            )

        n_keys_pos = booklet.utils.n_keys_pos
        local_init_bytes[n_keys_pos:n_keys_pos+4] = b'\x00\x00\x00\x00'

        ## The manifest this commit publishes: a replacement starts fresh
        ## (only this push's generations); otherwise the old manifest with
        ## successful groups re-pointed and emptied groups dropped.
        if num_groups is not None:
            if replace_pending:
                new_manifest = dict(new_gens)
            else:
                new_manifest = dict(pre_push_manifest)
                new_manifest.update(new_gens)
                for gid in emptied_gids:
                    new_manifest.pop(gid, None)
            index_bytes_for_commit = staged_index_bytes
        else:
            new_manifest = {}
            with remote_index._thread_lock:
                remote_index._file.seek(0)
                index_bytes_for_commit = remote_index._file.read()

        embedded_local_meta = journal.meta_pending
        meta_section = _build_meta_section_for_push(local_file, journal, remote_state, replace_pending, time_int_us)
        payload = build_db_payload(new_manifest, meta_section, index_bytes_for_commit)

        metadata = {
            'timestamp': str(time_int_us),
            'uuid': local_file.uuid.hex,
            'type': ebooklet_type,
            'init_bytes': base64.urlsafe_b64encode(local_init_bytes).decode(),
            'format_version': str(SUPPORTED_FORMAT_VERSION),
        }
        if num_groups is not None:
            metadata['num_groups'] = str(num_groups)

        ## The commit PUT is the point of no return: re-verify the write lock
        ## so a holder whose ticket was broken (another client's force_lock)
        ## aborts here instead of committing without mutual exclusion. All
        ## pending state stays journaled for a retry.
        if lock is not None and not lock.verify():
            raise LockLostError(
                "The write lock is no longer held (this session's lock ticket was broken by "
                'another client) - aborting the push before the commit. All pending changes '
                'are retained; re-open the file to re-acquire the lock and push again.'
            )

        resp = remote_session.put_db_object(payload, metadata=metadata)

        if resp.status // 100 != 2:
            raise urllib3.exceptions.HTTPError("The db object failed to upload. You need to rerun the push with force_push=True or the remote will be corrupted.")

        ## remove deletes in remote (only for legacy per-key mode). A raised
        ## delete failure propagates BEFORE the journal clearing below, so the
        ## pending deletes are retained for retry.
        if deletes and num_groups is None:
            remote_session.delete_objects(list(deletes))

        updated = True

        ## COMMIT SUCCEEDED. Apply the staged index mutations to the live
        ## sidecar (it now matches what the commit published)...
        if num_groups is not None:
            for key, entry in staged_entries.items():
                remote_index[key] = entry
            for key in committed_delete_keys:
                if key in remote_index:
                    del remote_index[key]
            remote_index.sync()

        ## ...record the committed remote state (manifest + metadata section +
        ## timestamp) in the persistent cache...
        remote_state.update_committed(new_manifest, meta_section, time_int_us)
        remote_state.persist(local_file)

        ## ...and only now clear the journal, for exactly the state this
        ## commit made durable (review-converged rule: never clear on a failed
        ## or skipped commit; a partially-failed replacement never commits).
        if replace_pending:
            committed_written = set(journal.written) if not failures else set()
            committed_deletes = set(journal.deletes) if not failures else set()
        elif num_groups is not None:
            committed_written = {k for k in journal.written if key_to_group_id(k, num_groups) not in failed_gids}
            committed_deletes = {k for k in journal.deletes if key_to_group_id(k, num_groups) not in failed_gids}
        else:
            committed_written = journal.written - set(failures)
            committed_deletes = set(journal.deletes)
        journal.clear_committed(committed_written, committed_deletes)
        if embedded_local_meta:
            journal.set_meta_pending(False)
        ## Record the storage-mode choice this commit materialized (tri-state:
        ## per-key is num_groups=None WITH num_groups_set=True).
        if not journal.num_groups_set:
            journal.set_num_groups(num_groups)
        journal.persist(local_file)

        ## Phase D - GC of the replaced/emptied OLD generations (exact keys).
        ## Failures are log-only: nothing references these objects any more
        ## (the commit already dropped them from the manifest and index;
        ## copy_remote is manifest-driven; fsck sweeps orphans).
        if num_groups is not None:
            if replace_pending:
                ## Replacement: sweep EVERYTHING in the namespace the new
                ## manifest does not reference - the old database's objects,
                ## and any orphans. Safe only while mutual exclusion holds,
                ## so re-verify the lock; on failure, abort the sweep (the
                ## commit already succeeded - the leftovers are orphans for
                ## fsck, never a correctness problem).
                if lock is not None and not lock.verify():
                    logger.warning('The write lock was lost after the replacement commit - skipping the old-object sweep (the leftovers are invisible orphans; run fsck to clean them up).')
                else:
                    expected = {group_obj_key(gid, gen) for gid, gen in new_manifest.items()}
                    try:
                        listed = remote_session.list_objects()
                        prefix_len = len(remote_session.write_db_key) + 1
                        sweep_keys = []
                        for obj in listed.iter_objects():
                            child = obj['key'][prefix_len:]
                            if child and child not in expected:
                                sweep_keys.append(child)
                        for child in sweep_keys:
                            err = remote_session.delete_object(child)
                            if err is not None:
                                logger.warning(f"Replacement sweep could not delete '{child}' (orphan; fsck will sweep): {err}")
                    except Exception as err:
                        logger.warning(f'Replacement sweep failed (leftovers are invisible orphans; run fsck): {err}')
            else:
                for gid, gen in new_gens.items():
                    old_gen = pre_push_manifest.get(gid)
                    if old_gen is not None and old_gen != gen:
                        err = remote_session.delete_object(group_obj_key(gid, old_gen))
                        if err is not None:
                            logger.warning(f"Could not GC replaced generation '{gid}.{old_gen}' (orphan; fsck will sweep): {err}")
                for gid in emptied_gids:
                    old_gen = pre_push_manifest.get(gid)
                    if old_gen is not None:
                        err = remote_session.delete_object(group_obj_key(gid, old_gen))
                        if err is not None:
                            logger.warning(f"Could not GC emptied group's generation '{gid}.{old_gen}' (orphan; fsck will sweep): {err}")

    if failures:
        return failures
    else:
        return updated


def indirect_copy_remote(source_session, target_session, source_key, target_key, source_bucket, dest_bucket):
    """

    """
    source_resp = source_session.get_object(source_key)

    if source_resp.status // 100 != 2:
        return source_resp

    target_resp = target_session.put_object(target_key, source_resp.stream, source_resp.metadata)

    return target_resp




















































