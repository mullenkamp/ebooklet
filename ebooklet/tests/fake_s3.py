"""
In-memory stand-in for s3func.S3Session, faithful to the semantics ebooklet
relies on (single-object PUT atomicity, string-prefix listing/deletes, ranged
GETs with 206/416, user metadata round-trip). ebooklet's real remote.py
S3SessionReader/S3SessionWriter classes are constructed over it, so every code
path above the HTTP layer is the real production code. No network is touched.

Adapted from the architecture-assessment harness
(architecture-assessment-harness-fake_s3.py at the repo root, kept verbatim as
an assessment artifact).

Known fidelity limits (these belong to the live test tier):
- ONE version per key: the store cannot reproduce all-versions purges, delete
  markers, or s3func's exact-key version-resolution filter (s3func s3.py:441).
- FakeLock never writes lock-ticket objects: tests asserting on the
  db_key + '.lock.' namespace must seed those keys into the store manually.
"""
import threading
import io
import uuid as _uuid

from ebooklet import remote


class FakeResp:
    def __init__(self, status, data=b'', metadata=None, error=None):
        self.status = status
        self.data = data
        self.metadata = metadata if metadata is not None else {}
        self.error = error if error is not None else {'status': status}
        self.stream = io.BytesIO(data)


class FakeListResp:
    def __init__(self, items):
        self.status = 200
        self._items = items
        self.error = None

    def iter_objects(self):
        yield from self._items


class FakeLock:
    """Single-process stand-in for the s3func bakery lock."""
    def __init__(self):
        self._held = False

    def acquire(self, blocking=True, timeout=-1, exclusive=True):
        self._held = True
        return True

    def release(self):
        self._held = False

    def other_locks(self):
        return {}

    def break_other_locks(self, timestamp=None):
        return []


class FakeS3Session:
    """Mimics s3func.S3Session over a shared in-memory dict {key: (bytes, metadata)}."""
    def __init__(self, store, store_lock=None, bucket='fake-bucket'):
        self.store = store
        self._lock = store_lock or threading.Lock()
        self.bucket = bucket
        self._session = {}          # s3session_finalizer calls ._session.clear()
        self._access_key_id = 'fake'
        self._access_key = 'fake'
        self.put_log = []           # every key ever PUT (survives deletes)

    # --- object ops -------------------------------------------------
    def put_object(self, key, obj, metadata=None, content_type=None):
        metadata = dict(metadata or {})
        if hasattr(obj, 'read'):
            obj = obj.read()
        data = bytes(obj)
        with self._lock:
            self.store[key] = (data, metadata)
            self.put_log.append(key)
        out_meta = dict(metadata)
        out_meta['version_id'] = _uuid.uuid4().hex
        return FakeResp(200, b'', out_meta)

    def get_object(self, key, version_id=None, range_start=None, range_end=None):
        with self._lock:
            entry = self.store.get(key)
        if entry is None:
            return FakeResp(404)
        data, metadata = entry
        if range_start is not None:
            if range_start >= len(data):
                return FakeResp(416)
            end = len(data) - 1 if range_end is None else min(range_end, len(data) - 1)
            return FakeResp(206, data[range_start:end + 1], dict(metadata))
        return FakeResp(200, data, dict(metadata))

    def head_object(self, key, version_id=None):
        with self._lock:
            entry = self.store.get(key)
        if entry is None:
            return FakeResp(404)
        return FakeResp(200, b'', dict(entry[1]))

    def delete_object(self, key, version_id=None):
        with self._lock:
            self.store.pop(key, None)
        return FakeResp(204)

    def delete_objects(self, keys=None, prefix=None, purge=True):
        # Mirrors s3func semantics: prefix -> list-by-string-prefix then delete.
        with self._lock:
            if prefix is not None:
                doomed = [k for k in self.store if k.startswith(prefix)]
            else:
                doomed = []
                for k in keys:
                    doomed.append(k['key'] if isinstance(k, dict) else k)
            for k in doomed:
                self.store.pop(k, None)
        return None

    def list_objects(self, prefix=None, start_after=None, delimiter=None, max_keys=None):
        with self._lock:
            items = [{'key': k, 'version_id': None} for k in sorted(self.store)
                     if prefix is None or k.startswith(prefix)]
        return FakeListResp(items)

    def list_object_versions(self, prefix=None, **kw):
        return self.list_objects(prefix=prefix)

    def copy_object(self, source_key, dest_key, source_version_id=None,
                    source_bucket=None, dest_bucket=None, metadata={}, content_type=None):
        with self._lock:
            entry = self.store.get(source_key)
            if entry is None:
                return FakeResp(404)
            self.store[dest_key] = entry
        return FakeResp(200)

    # --- lock -------------------------------------------------------
    def lock(self, key, lock_id=None, **kw):
        return FakeLock()


class FakeS3Connection(remote.S3Connection):
    """
    A remote.S3Connection whose open() returns REAL S3SessionReader/Writer
    objects backed by a FakeS3Session over the given store dict. Because it IS
    an S3Connection, check_remote_conn passes and the full open_ebooklet /
    open_rcg / push / pull production paths run hermetically.

    Share one `store` dict between connections to model one bucket
    (e.g. sibling databases 'mydb' and 'mydb2').
    """
    def __init__(self, store, db_key, bucket='fake-bucket', threads=4):
        super().__init__(
            access_key_id='fake-id',
            access_key='fake-key',
            db_key=db_key,
            bucket=bucket,
            endpoint_url='https://fake-s3.invalid',
            threads=threads,
            )
        self.store = store

    def open(self, flag: str = 'r'):
        sess = FakeS3Session(self.store, bucket=self.bucket)
        if flag == 'r':
            return remote.S3SessionReader(sess, self.db_key, self.threads)
        return remote.S3SessionWriter(sess, sess, self.db_key, self.db_key, self.threads)


def make_writer(store, db_key, threads=4):
    sess = FakeS3Session(store)
    return remote.S3SessionWriter(sess, sess, db_key, db_key, threads)


def make_reader(store, db_key, threads=4):
    sess = FakeS3Session(store)
    return remote.S3SessionReader(sess, db_key, threads)
