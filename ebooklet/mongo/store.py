"""
MongoStore: a read-only, per-request store that serves cfdb from MongoDB and fills missing chunks
from the ebooklet remote.

Build one per request: `cfdb.open_store_dataset(MongoStore(db, dataset_id, reader))`, and call
`store.verify()` before using what the request read. The store reads the dataset document once, at
construction, and keeps that document's `applied` pair and `rev` for the whole request. Nothing
persists in the process between requests; MongoDB is the only cache.

A read:
  1. index entries for the requested keys ($in);
  2. cached chunks ($in on (dataset, entry uuid, key, entry timestamp));
  3. misses fetched with ebooklet's own remote fetchers, inserted into the cache (insert-only,
     labelled with the timestamp of the bytes actually fetched), and memoised for the request.

The consistency rule: a request is answered only if no refresh applied a commit while it ran. The
store refuses to build while an apply is in progress, and verify() raises StaleIndexError if `rev`
moved or an apply started since construction - the request's metadata and its chunks might then
come from different commits (decoded values from neither). StaleIndexError is retryable WITH A NEW
STORE; a store that raised it stays stale.

Never returns None for a failed fetch (cfdb would read it as fill values). The rules:

- a key with no entry: re-read the dataset's `rev`; changed (or applying) -> StaleIndexError,
  unchanged -> genuinely absent, None.
- a per-key object whose timestamp differs from its entry's (overwritten in place since the index
  was copied, maybe not yet committed) -> StaleIndexError.
- a 404 (or a group member missing): re-read the entry (changed -> fetch it again; gone -> the
  absent rule), else refetch once, else one HEAD of the db object compared with the `applied` pair
  read at construction: equal -> RemoteIntegrityError, different -> StaleIndexError, HEAD 404 ->
  RemoteMissingError.
"""
import datetime
import logging
from concurrent.futures import ThreadPoolExecutor

import urllib3
from bson.binary import Binary
from pymongo.errors import BulkWriteError, PyMongoError

from ebooklet import remote, utils
from ebooklet.errors import ReadOnlyError, RemoteIntegrityError, RemoteMissingError, StaleIndexError
from ebooklet.mongo import schema
from ebooklet.mongo.schema import CHUNKS, DATASETS, ENTRIES, check_dataset_id, chunk_id, entry_id

logger = logging.getLogger(__name__)

_ENTRY_PROJECTION = {'_id': 0, 'k': 1, 'ts': 1, 'g': 1, 'n': 1, 'o': 1, 'l': 1, 'u': 1}


class _ObjectReader(remote.S3SessionReader):
    """An S3SessionReader that skips the HEAD its constructor normally sends: get_object/head_object only."""

    def __init__(self, read_session, read_db_key, threads):
        self._read_session = read_session
        self.read_db_key = read_db_key
        self.threads = threads


def object_reader(db_url, http_session=None, threads=8):
    """
    A reader for a remote's objects that makes no network call when built. Pass one
    `http_session` per process to share its connection pool across datasets and requests: an
    ``s3func.HttpSession(stream=False)`` (it is not tied to one URL). A streaming session
    (s3func's default, ``stream=True``) is refused: its responses carry no body and hold their pool
    connection, so the fetchers fail and the pool then blocks.

    Public remotes only: it takes a `db_url`, never credentials, so a store cannot read a private
    bucket (envlib's member buckets are public). To support private datasets, accept an
    S3Connection here and build the session with remote.create_s3_read_session from its keys.
    refresh() is not limited: it takes any S3Connection.
    """
    if http_session is None:
        http_session, db_url = remote.create_s3_read_session(db_url=db_url, threads=threads)
    elif getattr(http_session, '_stream', False):
        raise ValueError('object_reader needs a non-streaming session: build it with s3func.HttpSession(stream=False).')
    return _ObjectReader(http_session, db_url, threads)


class _Sink:
    """Collects what ebooklet's fetchers deliver (they call only .set)."""

    def __init__(self):
        self.items = {}

    def set(self, key, value, timestamp, encode_value=True):
        self.items[key] = (int(timestamp), bytes(value))


def _batches(seq):
    n = schema.IN_BATCH
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


class MongoStore:
    """
    The read-only store cfdb opens a dataset on (see the module docstring). Not shared between
    requests or threads.
    """

    writable = False

    def __init__(self, db, dataset_id, reader, *, threads=8):
        check_dataset_id(dataset_id)
        self._db = db
        self._d = dataset_id
        self._reader = reader
        self._threads = threads
        doc = db[DATASETS].find_one({'_id': dataset_id})
        if doc is None:
            raise RemoteMissingError(f'Dataset {dataset_id!r} is not in MongoDB (no refresh has run for it).')
        if doc.get('missing'):
            raise RemoteMissingError(f'Dataset {dataset_id!r}: its remote was missing at the last check.')
        applied = doc.get('applied')
        if applied is None or doc.get('applying'):
            raise StaleIndexError(
                f'Dataset {dataset_id!r}: a refresh is applying a new commit (or was interrupted); retry shortly.',
                dataset_id=dataset_id,
            )
        self._applied = (applied['uuid'], applied['ts'])
        self._rev = doc['rev']
        ## The remote's ebooklet type (e.g. 'EVariableLengthValue', 'RemoteConnGroup'). The store serves
        ## the bytes as stored whatever the type; what they mean is the caller's business.
        self.ebooklet_type = doc.get('ebooklet_type')
        section = doc.get('meta_section')
        self._meta_ts, self._meta = utils.parse_meta_section(bytes(section)) if section else (None, None)
        self._values = {}    # key -> bytes, or None for genuinely absent (memo for this request)

    def __repr__(self):
        return f'MongoStore({self._d!r}, applied={self._applied})'

    # ── the cfdb-facing API ──────────────────────────────────────────────────────────────────

    def get_metadata(self, include_timestamp=False):
        if include_timestamp and self._meta is not None:
            return self._meta, self._meta_ts
        return self._meta

    def verify(self):
        """
        Raise StaleIndexError if a refresh applied (or began applying) a commit since this store was
        built: what the request read may then mix two commits' metadata and chunks. Call it before
        using a request's results (an API: before sending the response).
        """
        if self._changed():
            raise StaleIndexError(
                f'Dataset {self._d!r}: a refresh applied a new commit during this request; retry with a new store.',
                dataset_id=self._d, applied=self._applied,
            )

    def get(self, key, default=None):
        if key not in self._values:
            self._fill([key])
        value = self._values[key]
        return default if value is None else value

    def load_items(self, keys=None):
        """Load the given keys into this request's memo. Raises on any failure; returns {}."""
        if keys is None:
            raise NotImplementedError(
                'MongoStore.load_items needs explicit keys (a whole-dataset load is not supported).'
            )
        wanted = [k for k in dict.fromkeys(keys) if k not in self._values]
        if wanted:
            self._fill(wanted)
        return {}

    def sync(self):
        pass

    def close(self):
        pass

    def map(self, *args, **kwargs):
        raise NotImplementedError(
            'map() is not supported on a MongoStore (cfdb loads the selection before calling it, so the data '
            'was already fetched); iterate the chunks instead.'
        )

    def prune(self, *args, **kwargs):
        raise ReadOnlyError('A MongoStore is read-only; prune() does not apply.')

    @property
    def _n_buckets(self):
        raise NotImplementedError(
            'copy() is not supported on a MongoStore dataset; open it with cfdb.open_edataset to copy it.'
        )

    # ── internals ────────────────────────────────────────────────────────────────────────────

    def _read_entries(self, keys):
        ids = [entry_id(self._d, k) for k in keys]
        found = {}
        for batch in _batches(ids):
            for doc in self._db[ENTRIES].find({'_id': {'$in': batch}}, _ENTRY_PROJECTION):
                found[doc.pop('k')] = doc
        return found

    def _changed(self):
        # rev alone suffices: an apply's claim bumps rev in the same write that sets `applying`,
        # and a store is never built while `applying` is set (mutant "_changed ignores applying"
        # survived every test for exactly this reason; review edr-mongo-code-1).
        doc = self._db[DATASETS].find_one({'_id': self._d}, {'rev': 1})
        return doc is None or doc['rev'] != self._rev

    def _absent(self, keys):
        """Keys with no entry: genuinely absent, unless a refresh changed the dataset meanwhile."""
        if keys and self._changed():
            raise StaleIndexError(
                f'Dataset {self._d!r}: a refresh applied a new commit during this request (keys {sorted(keys)[:5]} '
                'have no entry now); retry with a new store.',
                dataset_id=self._d, applied=self._applied,
            )
        for k in keys:
            self._values[k] = None

    def _fill(self, keys):
        found = self._read_entries(keys)
        self._absent([k for k in keys if k not in found])
        if not found:
            return
        ids = {chunk_id(self._d, e['u'], k, e['ts']): k for k, e in found.items()}
        id_list = list(ids)
        for batch in _batches(id_list):
            for doc in self._db[CHUNKS].find({'_id': {'$in': batch}}, {'v': 1}):
                self._values[ids[doc['_id']]] = bytes(doc['v'])
        misses = {k: e for k, e in found.items() if k not in self._values}
        if misses:
            self._fetch_and_heal(misses)

    def _fetch(self, entries):
        """Fetch the entries' values from the remote. Returns (sink items, {key: failure})."""
        sink = _Sink()
        failures = {}
        groups, per_key = {}, []
        for k, e in entries.items():
            if e['g'] is None:
                per_key.append(k)
            elif e['n'] is None:
                # the index names a group its manifest does not list: no object to fetch
                failures[k] = utils.MissingRemoteObject(f"{e['g']}.<unmanifested>", [k])
            else:
                groups.setdefault((e['g'], e['n']), []).append((k, e['o'], e['l'], e['ts']))
        tasks = [
            (utils.get_remote_group_values, (g, n, infos, sink, self._reader))
            for (g, n), infos in groups.items() if infos
        ]
        tasks += [(utils.get_remote_value, (sink, k, self._reader)) for k in per_key]
        if tasks:
            with ThreadPoolExecutor(max_workers=max(1, min(self._threads, len(tasks)))) as pool:
                results = list(pool.map(lambda t: t[0](*t[1]), tasks))
            for res in results:
                if not res:
                    continue
                if isinstance(res, utils.MissingRemoteObject):
                    for k in res.keys:
                        failures[k] = res
                else:
                    raise urllib3.exceptions.HTTPError(f'Dataset {self._d!r}: remote fetch failed: {res}')
        for k in entries:
            if k not in sink.items and k not in failures:
                failures[k] = utils.MissingRemoteObject(f'<not delivered: {k}>', [k])
        return sink.items, failures

    def _fetch_and_heal(self, entries):
        items, failures = self._fetch(entries)
        self._store(entries, items)
        if not failures:
            return

        ## 1. Re-read the failed keys' entries: a refresh may have moved them (changed) or removed them.
        fresh = self._read_entries(list(failures))
        self._absent([k for k in failures if k not in fresh])
        retry = {k: fresh[k] for k in failures if k in fresh}
        if not retry:
            return

        ## 2. Refetch once (with the fresh entries where they changed).
        items, failures = self._fetch(retry)
        self._store(retry, items)
        if not failures:
            return

        ## 3. One HEAD, compared with the applied pair this request was built on.
        head = self._reader.head_object()
        if head.status == 404:
            raise RemoteMissingError(f'Dataset {self._d!r}: the remote database no longer exists.')
        if head.status != 200:
            raise urllib3.exceptions.HTTPError(f'Dataset {self._d!r}: db-object HEAD failed: {head.error}')
        fields = remote.classify_db_metadata(head.metadata)
        if (fields['uuid'].hex, fields['timestamp']) == self._applied:
            raise RemoteIntegrityError(
                f'Dataset {self._d!r}: the remote index claims key(s) {sorted(failures)[:5]} but their objects are '
                f'missing, and the remote has not committed since this index ({failures[next(iter(failures))]!r}).'
            )
        raise StaleIndexError(
            f'Dataset {self._d!r}: chunk(s) {sorted(failures)[:5]} 404\'d and the remote has committed since '
            'the index this request uses; retry after the next refresh.',
            dataset_id=self._d, applied=self._applied,
        )

    def _store(self, entries, items):
        """
        Insert fetched values into the chunk cache (best effort, each labelled with its own
        timestamp) and memoise them - except a per-key object whose timestamp differs from its
        entry's: it was overwritten in place since the index was copied (maybe not yet committed),
        so it does not belong to this request's commit. It is cached under its own label, not
        memoised, and the request is told to retry.
        """
        now = datetime.datetime.now(datetime.timezone.utc)
        docs = []
        ahead = []
        for k, (ts, value) in items.items():
            entry = entries.get(k)
            if entry is not None and entry['g'] is None and ts != entry['ts']:
                ahead.append(k)
            else:
                self._values[k] = value
            if entry is None:
                continue
            if len(value) > schema.MONGO_DOC_LIMIT:
                logger.warning(
                    f'Dataset {self._d!r}: chunk {k!r} is {len(value):,} B, over the MongoDB document limit; '
                    'served uncached.'
                )
                continue
            docs.append({'_id': chunk_id(self._d, entry['u'], k, ts), 'v': Binary(value), 'n': len(value), 't': now})
        if docs:
            try:
                self._db[CHUNKS].insert_many(docs, ordered=False)
            except BulkWriteError as err:
                other = [w for w in err.details.get('writeErrors', []) if w.get('code') != 11000]
                if other:
                    logger.warning(
                        f'Dataset {self._d!r}: {len(other)} chunk(s) not cached '
                        f'({other[0].get("errmsg")}); served anyway.'
                    )
            except PyMongoError as err:
                logger.warning(f'Dataset {self._d!r}: chunk cache insert failed ({err}); served anyway.')
        if ahead:
            raise StaleIndexError(
                f'Dataset {self._d!r}: per-key object(s) {sorted(ahead)[:5]} were overwritten since the index this '
                'request uses (a push in progress, or not yet refreshed); retry after the next refresh.',
                dataset_id=self._d, applied=self._applied,
            )
