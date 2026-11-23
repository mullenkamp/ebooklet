"""
Shared helpers for the ebooklet.mongo tests and the writer-order tests.

- `RecordingSession` / `RecordingConnection`: the fake S3 with an operation log and a hook that runs
  just before every PUT, so a test can stage a read at the commit point of a real push.
- `CountingSession`: a fake S3 session that counts remote calls by kind.
- `mongo_db`: a real MongoDB database, unique per test and dropped afterwards. It needs
  EBOOKLET_TEST_MONGO_URI (CI sets it). Locally the tests skip with a visible reason when it is
  unset; with CI set they FAIL instead, so a misconfigured CI can never pass by skipping.

  docker run -d --rm --name ebooklet-test-mongo -p 127.0.0.1:27019:27017 --tmpfs /data/db mongo:8.0
  EBOOKLET_TEST_MONGO_URI=mongodb://127.0.0.1:27019 uv run pytest
"""
import collections
import os
import uuid as _uuid

import pymongo
import pytest

from ebooklet import remote
from ebooklet.tests import fake_s3

MONGO_URI = os.environ.get('EBOOKLET_TEST_MONGO_URI')


class RecordingSession(fake_s3.FakeS3Session):
    """
    A FakeS3Session that appends ('PUT'|'DELETE', key) to `ops` (shared per connection) and calls
    `before_put(key)` just before each PUT is stored. Deletes are logged per key.
    """

    def __init__(self, store, ops, before_put=None, **kw):
        super().__init__(store, **kw)
        self.ops = ops
        self.before_put = before_put

    def put_object(self, key, obj, metadata=None, content_type=None, expires_at=None):
        if self.before_put is not None:
            self.before_put(key)
        self.ops.append(('PUT', key))
        return super().put_object(key, obj, metadata=metadata, content_type=content_type, expires_at=expires_at)

    def delete_object(self, key, version_id=None):
        self.ops.append(('DELETE', key))
        return super().delete_object(key, version_id=version_id)

    def delete_objects(self, keys=None, prefix=None, purge=True):
        with self._lock:
            if prefix is not None:
                doomed = sorted(k for k in self.store if k.startswith(prefix))
            else:
                doomed = [k['key'] if isinstance(k, dict) else k for k in keys]
        for k in doomed:
            self.ops.append(('DELETE', k))
        return super().delete_objects(keys=keys, prefix=prefix, purge=purge)


class RecordingConnection(fake_s3.FakeS3Connection):
    """A FakeS3Connection whose sessions record into `self.ops` and call `self.before_put`."""

    def __init__(self, store, db_key, **kw):
        super().__init__(store, db_key, **kw)
        self.ops = []
        self.before_put = None

    def open(self, flag='r'):
        sess = RecordingSession(self.store, self.ops, before_put=self._hook, bucket=self.bucket)
        if flag == 'r':
            return remote.S3SessionReader(sess, self.db_key, self.threads)
        return remote.S3SessionWriter(sess, sess, self.db_key, self.db_key, self.threads)

    def _hook(self, key):
        if self.before_put is not None:
            self.before_put(key)


class CountingSession(fake_s3.FakeS3Session):
    """
    A FakeS3Session counting remote calls (calls['GET'], calls['HEAD'], calls['PUT']), with optional
    hooks: `on_get(key)` / `on_head(key)` run before the call and may return a FakeResp to answer
    instead of the store.
    """

    def __init__(self, store, on_get=None, on_head=None, **kw):
        super().__init__(store, **kw)
        self.calls = collections.Counter()
        self.on_get = on_get
        self.on_head = on_head

    def get_object(self, key, version_id=None, range_start=None, range_end=None):
        self.calls['GET'] += 1
        if self.on_get is not None:
            resp = self.on_get(key)
            if resp is not None:
                return resp
        return super().get_object(key, version_id=version_id, range_start=range_start, range_end=range_end)

    def head_object(self, key, version_id=None):
        self.calls['HEAD'] += 1
        if self.on_head is not None:
            resp = self.on_head(key)
            if resp is not None:
                return resp
        return super().head_object(key, version_id=version_id)

    def put_object(self, key, obj, metadata=None, content_type=None, expires_at=None):
        self.calls['PUT'] += 1
        return super().put_object(key, obj, metadata=metadata, content_type=content_type, expires_at=expires_at)


class HookConnection(fake_s3.FakeS3Connection):
    """A FakeS3Connection whose readers use a CountingSession with the given hooks (the refresh's remote)."""

    def __init__(self, store, db_key, on_get=None, on_head=None, **kw):
        super().__init__(store, db_key, **kw)
        self.on_get = on_get
        self.on_head = on_head
        self.sessions = []

    def open(self, flag='r'):
        if flag != 'r':
            return super().open(flag)
        sess = CountingSession(self.store, on_get=self.on_get, on_head=self.on_head, bucket=self.bucket)
        self.sessions.append(sess)
        return remote.S3SessionReader(sess, self.db_key, self.threads)


class Crash(Exception):
    """Raised by CrashAfter to stop a refresh between two of its writes."""


class CrashAfter:
    """
    A pymongo Database proxy that performs the first call of `collection.method` and then raises
    Crash - a refresh interrupted right after that write.
    """

    def __init__(self, db, collection, method):
        self._db = db
        self._collection = collection
        self._method = method

    def __getitem__(self, name):
        coll = self._db[name]
        return _CrashCollection(coll, self._method) if name == self._collection else coll


class _CrashCollection:
    def __init__(self, coll, method):
        self._coll = coll
        self._method = method

    def __getattr__(self, name):
        attr = getattr(self._coll, name)
        if name != self._method:
            return attr

        def crash_after(*args, **kwargs):
            attr(*args, **kwargs)
            raise Crash(f'{self._coll.name}.{name}')

        return crash_after


class CommandRecorder(pymongo.monitoring.CommandListener):
    """Records (command name, collection) for every command a client sends."""

    def __init__(self):
        self.commands = []

    def started(self, event):
        target = event.command.get(event.command_name)
        self.commands.append((event.command_name, target if isinstance(target, str) else None))

    def succeeded(self, event):
        pass

    def failed(self, event):
        pass


def recording_db(db):
    """(the same database through a client that records its commands, the recorder)."""
    rec = CommandRecorder()
    client = pymongo.MongoClient(MONGO_URI, event_listeners=[rec], tz_aware=True)
    return client[db.name], rec, client


def require_mongo():
    """Skip (locally) or fail (CI) when no test MongoDB is configured."""
    if MONGO_URI:
        return
    if os.environ.get('CI'):
        pytest.fail('CI is set but EBOOKLET_TEST_MONGO_URI is not: the MongoDB tests must run in CI.')
    pytest.skip('EBOOKLET_TEST_MONGO_URI is not set; start a mongod to run the MongoDB tests (see mongo_support.py)')


@pytest.fixture(scope='session')
def mongo_client():
    require_mongo()
    client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=3000, tz_aware=True)
    client.admin.command('ping')
    yield client
    client.close()


@pytest.fixture
def mongo_db(mongo_client):
    db = mongo_client[f'ebooklet_test_{_uuid.uuid4().hex[:12]}']
    yield db
    mongo_client.drop_database(db.name)
