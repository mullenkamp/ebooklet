"""
The MongoDB collections of ebooklet's MongoDB read path, their validators, and ensure_schema().

Three collections, all keyed by a dataset id (any string without '|', e.g. an envlib dataset_id):

- `datasets`: one document per dataset - the remote's metadata section, its storage kind and
  format, the (uuid, timestamp) pair of the commit whose index the entries reflect (`applied`), a
  revision counter bumped at the start of every apply (`rev`), whether an apply is in progress
  (`applying`), when the remote was last checked (`checked_at`), and whether it was last seen
  missing (`missing`).
- `entries`: one document per remote-index key - where the value lives (group id, generation,
  offset, length, or per-key) and its timestamp.
- `chunks`: the insert-only value cache, one document per (dataset, remote uuid, key, timestamp).
  A TTL index on the fetch time `t` expires them.

Every integer is validated as int OR long: pymongo picks the BSON width by value, and offsets and
lengths reach 2**32 - 1. `t` must be a BSON date, or the TTL monitor never expires the document.

Block compression (WiredTiger, fixed when a collection is created): zstd for `datasets` and
`entries`, about 25% smaller than the default snappy on real indexes; none for `chunks`, whose
bytes are already compressed by cfdb, so compressing them again saves about 1% and costs CPU on
every page (measured 2026-11-24 on 167k real entries and 620 real chunks).
"""
import logging

from pymongo.errors import CollectionInvalid

logger = logging.getLogger(__name__)

DATASETS = 'datasets'
ENTRIES = 'entries'
CHUNKS = 'chunks'

## MongoDB's document limit is 16 MiB; keep a margin for the other fields and the id.
MONGO_DOC_LIMIT = 16 * 1024 * 1024 - 64 * 1024
## Ids per $in query: well under the 16 MiB query-document limit at real key lengths.
IN_BATCH = 50_000

TTL_INDEX = 't_ttl'

COMPRESSORS = {DATASETS: 'zstd', ENTRIES: 'zstd', CHUNKS: 'none'}

_INT = {'bsonType': ['int', 'long']}
_INT_OR_NULL = {'bsonType': ['int', 'long', 'null']}
_STR_OR_NULL = {'bsonType': ['string', 'null']}

VALIDATORS = {
    DATASETS: {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'db_url', 'rev', 'missing'],
            'properties': {
                '_id': {'bsonType': 'string'},
                'db_url': _STR_OR_NULL,
                'applied': {
                    'bsonType': ['object', 'null'],
                    'required': ['uuid', 'ts'],
                    'properties': {'uuid': {'bsonType': 'string'}, 'ts': _INT},
                },
                'rev': _INT,
                'uuid': _STR_OR_NULL,
                'storage_kind': _STR_OR_NULL,
                'ebooklet_type': _STR_OR_NULL,
                'format_version': _INT_OR_NULL,
                'meta_section': {'bsonType': ['binData', 'null']},
                'checked_at': {'bsonType': ['date', 'null']},
                'missing': {'bsonType': 'bool'},
                'applying': {'bsonType': 'bool'},
            },
        }
    },
    ENTRIES: {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'd', 'k', 'ts', 'g', 'n', 'o', 'l', 'u'],
            'properties': {
                '_id': {'bsonType': 'string'},
                'd': {'bsonType': 'string'},
                'k': {'bsonType': 'string'},
                'ts': _INT,
                'g': _INT_OR_NULL,
                'n': _STR_OR_NULL,
                'o': _INT,
                'l': _INT,
                'u': {'bsonType': 'string'},
            },
        }
    },
    CHUNKS: {
        '$jsonSchema': {
            'bsonType': 'object',
            'required': ['_id', 'v', 'n', 't'],
            'properties': {
                '_id': {'bsonType': 'string'},
                'v': {'bsonType': 'binData'},
                'n': _INT,
                't': {'bsonType': 'date'},
            },
        }
    },
}


def entry_id(dataset_id, key):
    return f'{dataset_id}|{key}'


def chunk_id(dataset_id, uuid_hex, key, ts):
    return f'{dataset_id}|{uuid_hex}|{key}|{ts}'


def check_dataset_id(dataset_id):
    if not isinstance(dataset_id, str) or not dataset_id or '|' in dataset_id:
        raise ValueError(f"dataset_id must be a non-empty string without '|', got {dataset_id!r}.")


def block_compressor(db, name):
    """The WiredTiger block compressor a collection was created with, or None if not reported."""
    stats = list(db[name].aggregate([{'$collStats': {'storageStats': {}}}]))
    creation = stats[0].get('storageStats', {}).get('wiredTiger', {}).get('creationString', '') if stats else ''
    for part in creation.split(','):
        if part.startswith('block_compressor='):
            return part.split('=', 1)[1] or 'none'
    return None


def ensure_schema(db, chunk_ttl_seconds):
    """
    Create (or update) the three collections, their validators and indexes. Idempotent; a changed
    chunk_ttl_seconds is applied to the existing TTL index with collMod (create_index would refuse
    with IndexOptionsConflict).

    New collections get the block compressors in COMPRESSORS. An existing collection keeps the
    compressor it was created with (MongoDB cannot change it); a mismatch is logged as a warning.
    To change it, drop the collection: `chunks` is only a cache, and `entries` is rebuilt by the
    next refresh of each dataset.
    """
    if isinstance(chunk_ttl_seconds, bool) or not isinstance(chunk_ttl_seconds, int) or chunk_ttl_seconds < 1:
        raise ValueError(f'chunk_ttl_seconds must be a positive int, got {chunk_ttl_seconds!r}.')
    for name, validator in VALIDATORS.items():
        wanted = COMPRESSORS[name]
        try:
            db.create_collection(
                name, validator=validator,
                storageEngine={'wiredTiger': {'configString': f'block_compressor={wanted}'}},
            )
        except CollectionInvalid:
            db.command('collMod', name, validator=validator, validationLevel='strict')
            actual = block_compressor(db, name)
            if actual is not None and actual != wanted:
                logger.warning(
                    f'Collection {db.name}.{name} uses block compressor {actual!r}, not {wanted!r}; it keeps '
                    'it until the collection is dropped and recreated (see ensure_schema).'
                )
    db[ENTRIES].create_index([('d', 1)], name='d_1')
    existing = db[CHUNKS].index_information().get(TTL_INDEX)
    if existing is None:
        db[CHUNKS].create_index([('t', 1)], name=TTL_INDEX, expireAfterSeconds=chunk_ttl_seconds)
    elif existing.get('expireAfterSeconds') != chunk_ttl_seconds:
        db.command('collMod', CHUNKS, index={'name': TTL_INDEX, 'expireAfterSeconds': chunk_ttl_seconds})
