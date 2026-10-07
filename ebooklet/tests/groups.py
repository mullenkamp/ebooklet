"""
Helpers for tests of write-order grouped storage (format 3).

Group membership is no longer a function of the key: the writer allocates it
at push time, in local-file write order, and records the gid in each remote-
index entry. Tests therefore construct "same group" / "different group"
situations through write order and pushes, and read the outcome back from the
index - every such construction must ASSERT its own precondition (gid_of(a) ==
gid_of(b), or !=), so a change to the allocator cannot make the test vacuous.
"""
import io

import booklet

from ebooklet import utils

## A small packing target for tests: a few short entries per group (an entry
## for a 2-char key and a 2-byte value packs to 17 bytes; the group header is
## 4), so a dozen keys span several groups.
TEST_GB = 64


def gid_of(eb, key):
    """The gid the session's remote-index copy records for key (None if the
    key has no index entry or the entry is per-key)."""
    entry = eb._remote_index.get(key)
    if entry is None:
        return None
    return utils.decode_index_entry(entry)[1]


def members_by_gid(eb):
    """{gid: sorted keys} from the session's remote-index copy."""
    out = {}
    for key, entry in eb._remote_index.items():
        gid = utils.decode_index_entry(entry)[1]
        out.setdefault(gid, []).append(key)
    return {gid: sorted(keys) for gid, keys in out.items()}


def remote_index_gids(store, db_key):
    """{key: gid} from the COMMITTED db object in a fake store."""
    _manifest, _meta, index_bytes = utils.parse_db_payload(store[db_key][0])
    f = booklet.FixedLengthValue(io.BytesIO(bytes(index_bytes)), 'r')
    try:
        return {k: utils.decode_index_entry(v)[1] for k, v in f.items()}
    finally:
        f.close()


def remote_manifest(store, db_key):
    """The committed manifest {gid: gen} in a fake store."""
    return utils.parse_db_payload(store[db_key][0])[0]


def group_objects(store, db_key):
    """{gid: object key} of the group objects present in a fake store."""
    prefix = db_key + '/'
    out = {}
    for k in store:
        if k.startswith(prefix):
            child = k[len(prefix):]
            gid, _, _gen = child.partition('.')
            if gid.isdigit():
                out[int(gid)] = k
    return out
