"""
refresh(): copy one dataset's remote index and metadata from its ebooklet remote into MongoDB.

Run by ONE process (the worker). It holds no state between calls: each refresh diffs the remote's
current index against the entries already in MongoDB.

The order of writes is the contract readers (ebooklet.mongo.store) rely on:

  0. claim: increment `rev` and set `applying` (after the GET, parse and diff, so the window in
     which readers must retry covers only the writes below);
  1. upsert the changed and new entries;
  2. update the dataset document (metadata section, uuid, kind, format);
  3. delete the removed entries;
  4. set `applied` = the commit's (uuid, timestamp) and `checked_at`, and clear `applying` - only
     if `rev` is still the value step 0 claimed (otherwise another refresh interleaved: `applied`
     is cleared and the next poll redoes the diff).

So the metadata never runs ahead of the entries; a request that overlaps an apply sees `rev` move
or `applying` set, and retries; and a crash at any step is redone by the next poll (`applied` is
written last, and a left-over `applying` forces the redo).

EXACTLY ONE refresh process per dataset. Two overlapping refreshes can leave MongoDB permanently
inconsistent with the remote while `applied` matches it (plan review `edr-mongo-code-1`: the older
refresh's unconditional entry writes and deletes land around the newer one's checks). Supporting
concurrent refreshers would need per-entry apply tags with conditional writes, or a fenced lease;
see docs/ops.md.

Commit timestamps are the writer's wall clock and are NOT monotonic: a commit is "new" when its
(uuid, timestamp) pair DIFFERS from `applied`, never when it is "newer".

A dataset document gets an incarnation token `inc` (a random hex string) when it is first inserted.
If the document is deleted and recreated, `rev` restarts from the beginning but `inc` differs, so a
store built on the old document still sees the change (plan review `edr-worker-plan-2`).
"""
import datetime
import io
import logging
import uuid

import booklet
import urllib3
from bson.binary import Binary
from pymongo import ReplaceOne, ReturnDocument

from ebooklet import remote, utils
from ebooklet.mongo import schema
from ebooklet.mongo.schema import DATASETS, ENTRIES, check_dataset_id, entry_id

logger = logging.getLogger(__name__)

_ENTRY_FIELDS = ('ts', 'g', 'n', 'o', 'l', 'u')
WRITE_BATCH = 10_000


def _now():
    return datetime.datetime.now(datetime.timezone.utc)


def _parse_entries(index_bytes, manifest, uuid_hex):
    """{key: entry} from a committed index, skipping the metadata and reserved slots."""
    entries = {}
    unmanifested = []
    f = booklet.FixedLengthValue(io.BytesIO(bytes(index_bytes)), 'r')
    try:
        for key, value in f.items():
            if key == utils.metadata_key_str or key in utils.reserved_key_strs:
                continue
            ts, gid, offset, length = utils.decode_index_entry(value)
            gen = None if gid is None else manifest.get(gid)
            if gid is not None and gen is None:
                unmanifested.append(key)
            entries[key] = {'ts': ts, 'g': gid, 'n': gen, 'o': offset, 'l': length, 'u': uuid_hex}
    finally:
        f.close()
    return entries, unmanifested


def _current_entries(db, dataset_id):
    projection = {'_id': 0, 'k': 1, **{f: 1 for f in _ENTRY_FIELDS}}
    return {doc.pop('k'): doc for doc in db[ENTRIES].find({'d': dataset_id}, projection)}


def _mark_missing(db, dataset_id, db_url, checked):
    db[DATASETS].update_one(
        {'_id': dataset_id},
        {
            '$set': {'missing': True, 'checked_at': checked, 'db_url': db_url},
            '$setOnInsert': {'rev': 0, 'applied': None, 'inc': uuid.uuid4().hex},
        },
        upsert=True,
    )


def refresh(db, dataset_id, remote_conn, *, db_url=None):
    """
    Bring MongoDB's copy of one dataset up to date with its remote.

    Parameters
    ----------
    db : pymongo.database.Database
        The database ensure_schema() prepared.
    dataset_id : str
        The dataset's id in MongoDB (no '|').
    remote_conn : ebooklet.S3Connection
        The remote. It must not be served through a cache that holds db-object HEADs, GETs or 404s.
    db_url : str, optional
        Recorded in the dataset document; defaults to remote_conn.db_url.

    Returns
    -------
    dict
        {'status': 'nochange' | 'applied' | 'missing' | 'stale_get' | 'interleaved',
         'upserted': int, 'removed': int, 'unmanifested': int, 'applied': (uuid_hex, ts) | None}

    A failed refresh (an HTTP error, an unsupported format) raises and leaves `checked_at`
    untouched, so a stalled dataset stays visible.
    """
    check_dataset_id(dataset_id)
    if db_url is None:
        db_url = getattr(remote_conn, 'db_url', None)
    result = {'status': None, 'upserted': 0, 'removed': 0, 'unmanifested': 0, 'applied': None}
    checked = _now()

    ## 1. HEAD (inside open): the format gate, and the commit's pair.
    session = remote_conn.open('r')
    if not session.initialized:
        _mark_missing(db, dataset_id, db_url, checked)
        result['status'] = 'missing'
        return result
    if session.storage_kind == utils.STORAGE_LEGACY:
        raise utils.UnsupportedFormatError(
            f'Dataset {dataset_id!r}: the remote is a legacy (format 1 or hash-grouped) database, which '
            'ebooklet does not read.'
        )
    doc = db[DATASETS].find_one({'_id': dataset_id}) or {}
    applied = doc.get('applied')
    applied_pair = (applied['uuid'], applied['ts']) if applied else None
    ## A left-over `applying` (a crashed apply) is never "no change": the apply is redone.
    if applied_pair == (session.uuid.hex, session.timestamp) and not doc.get('applying'):
        db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'missing': False, 'checked_at': checked}})
        result['status'] = 'nochange'
        result['applied'] = applied_pair
        return result

    ## 2. GET: the stamp, uuid and format come from the GET's own metadata (atomic with the body).
    resp = session.get_object()
    if resp.status == 404:
        _mark_missing(db, dataset_id, db_url, checked)
        result['status'] = 'missing'
        return result
    if resp.status not in (200, 206):
        raise urllib3.exceptions.HTTPError(f'Dataset {dataset_id!r}: db object GET failed: {resp.error}')
    fields = remote.classify_db_metadata(resp.metadata)
    kind = remote.storage_kind_of(fields['format_version'], fields['legacy_num_groups'])
    if kind == utils.STORAGE_LEGACY:
        raise utils.UnsupportedFormatError(
            f'Dataset {dataset_id!r}: the remote is a legacy (format 1 or hash-grouped) database.'
        )
    uuid_hex = fields['uuid'].hex
    pair = (uuid_hex, fields['timestamp'])
    if pair == applied_pair and not doc.get('applying'):
        ## The HEAD showed another commit but the GET returned the applied one: a cache in front of
        ## the remote is serving one of them stale. Nothing to apply; checked_at is NOT advanced.
        logger.warning(
            f'Dataset {dataset_id!r}: the db-object HEAD and GET disagree (HEAD '
            f'{session.uuid.hex}/{session.timestamp}, GET {pair}); is a cache in front of the remote?'
        )
        db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'missing': False}})
        result['status'] = 'stale_get'
        result['applied'] = applied_pair
        return result

    manifest, meta_section, index_bytes = utils.parse_db_payload(resp.data)
    new, unmanifested = _parse_entries(index_bytes, manifest, uuid_hex)
    if unmanifested:
        logger.error(
            f'Dataset {dataset_id!r}: {len(unmanifested)} index entries name a group the manifest does '
            f'not list (e.g. {unmanifested[:3]}); reads of them will fail as integrity faults.'
        )
    cur = _current_entries(db, dataset_id)
    changed = [k for k, e in new.items() if cur.get(k) != e]
    removed = [k for k in cur if k not in new]

    ## Apply step 0: claim. Requests built from now on are refused, and requests already running
    ## see rev move; the window is the writes below only.
    claimed = db[DATASETS].find_one_and_update(
        {'_id': dataset_id},
        {
            '$inc': {'rev': 1},
            '$set': {'applying': True, 'db_url': db_url},
            '$setOnInsert': {'applied': None, 'checked_at': None, 'missing': False, 'inc': uuid.uuid4().hex},
        },
        upsert=True,
        return_document=ReturnDocument.AFTER,
    )
    rev = claimed['rev']

    ## Apply step 1: upsert changed and new entries.
    ops = [
        ReplaceOne({'_id': entry_id(dataset_id, k)}, {'d': dataset_id, 'k': k, **new[k]}, upsert=True)
        for k in changed
    ]
    for i in range(0, len(ops), WRITE_BATCH):
        db[ENTRIES].bulk_write(ops[i:i + WRITE_BATCH], ordered=False)

    ## Apply step 2: the dataset document.
    db[DATASETS].update_one(
        {'_id': dataset_id},
        {
            '$set': {
                'uuid': uuid_hex,
                'storage_kind': kind,
                'ebooklet_type': fields['type'],
                'format_version': fields['format_version'],
                'meta_section': Binary(bytes(meta_section)) if meta_section else None,
                'missing': False,
            },
        },
    )

    ## Apply step 3: delete the removed entries.
    ids = [entry_id(dataset_id, k) for k in removed]
    for i in range(0, len(ids), schema.IN_BATCH):
        db[ENTRIES].delete_many({'_id': {'$in': ids[i:i + schema.IN_BATCH]}})

    ## Apply step 4: the applied pair, last - only if no other refresh bumped rev meanwhile.
    done = db[DATASETS].update_one(
        {'_id': dataset_id, 'rev': rev},
        {'$set': {'applied': {'uuid': uuid_hex, 'ts': fields['timestamp']}, 'checked_at': checked, 'applying': False}},
    )
    result.update(upserted=len(changed), removed=len(removed), unmanifested=len(unmanifested))
    if done.matched_count == 0:
        logger.warning(f'Dataset {dataset_id!r}: another refresh interleaved with this one; the next poll redoes it.')
        db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'applied': None}})
        result['status'] = 'interleaved'
        return result
    result['status'] = 'applied'
    result['applied'] = pair
    return result
