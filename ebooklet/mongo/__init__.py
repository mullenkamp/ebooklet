"""
ebooklet's MongoDB read path (install with `pip install 'ebooklet[mongo]'`).

- `refresh(db, dataset_id, remote_conn)`: copy one dataset's remote index and metadata into
  MongoDB. Run by one worker process.
- `MongoStore(db, dataset_id, reader)`: a read-only, per-request store cfdb opens a dataset on
  (`cfdb.open_store_dataset`). It reads everything from MongoDB and fills missing chunks from the
  remote with `object_reader(db_url, http_session)`.
- `ensure_schema(db, chunk_ttl_seconds)`: create the collections, validators and indexes.

Design and operating rules: ebooklet/planning/mongo-read-path-plan.md and docs/ops.md.
"""
try:
    import pymongo  # noqa: F401
except ImportError as err:  # pragma: no cover - exercised by a subprocess test
    raise ImportError("ebooklet.mongo needs pymongo: install it with pip install 'ebooklet[mongo]'.") from err

from ebooklet.mongo.refresh import refresh
from ebooklet.mongo.schema import CHUNKS, DATASETS, ENTRIES, ensure_schema
from ebooklet.mongo.store import MongoStore, object_reader

__all__ = ['refresh', 'MongoStore', 'object_reader', 'ensure_schema', 'DATASETS', 'ENTRIES', 'CHUNKS']
