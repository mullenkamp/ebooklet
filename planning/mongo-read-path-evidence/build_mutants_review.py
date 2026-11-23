"""Mutants for code review edr-mongo-code-1: the arms' survivors and the new guards."""
import json
T = ["ebooklet/tests/test_mongo_refresh.py", "ebooklet/tests/test_mongo_store.py", "ebooklet/tests/test_mongo_cfdb.py"]
R, S, SC = "ebooklet/mongo/refresh.py", "ebooklet/mongo/store.py", "ebooklet/mongo/schema.py"
M = []
def m(name, f, old, new): M.append({"name": name, "file": f, "old": old, "new": new, "tests": T})
# new guards
m("verify never raises", S, "        if self._changed():\n            raise StaleIndexError(\n                f'Dataset {self._d!r}: a refresh applied",
  "        if False:\n            raise StaleIndexError(\n                f'Dataset {self._d!r}: a refresh applied")
m("construction ignores applying", S, "if applied is None or doc.get('applying'):", "if applied is None:")
m("claim does not set applying", R, "'$set': {'applying': True, 'db_url': db_url},", "'$set': {'applying': False, 'db_url': db_url},")
m("step 4 does not clear applying", R, "'checked_at': checked, 'applying': False}}", "'checked_at': checked}}")
m("nochange despite applying", R, "session.timestamp) and not doc.get('applying'):", "session.timestamp):")
m("_changed never reports a change", S, "        return doc is None or doc['rev'] != self._rev", "        return False")
m("per-key ts mismatch served", S, "entry['g'] is None and ts != entry['ts']", "False")
m("streaming session accepted", S, "elif getattr(http_session, '_stream', False):", "elif False:")
# the arms' survivors
m("heal refetches with the old entry", S, "retry = {k: fresh[k] for k in failures if k in fresh}", "retry = {k: entries[k] for k in failures if k in fresh}")
m("group fetch keyed by gid only", S, "groups.setdefault((e['g'], e['n']), [])",
  "groups.setdefault((e['g'], max(x['n'] for x in entries.values() if x['g'] == e['g'] and x['n'])), [])")
m("store builds on applied=None", S, "if applied is None or doc.get('applying'):", "if doc.get('applying'):")
m("classification HEAD non-200 ignored", S, "        if head.status != 200:\n            raise", "        if False:\n            raise")
m("chunk-GET error swallowed", S, "raise urllib3.exceptions.HTTPError(f'Dataset {self._d!r}: remote fetch failed: {res}')", "pass")
m("store batches: first batch only", S, "    for i in range(0, len(seq), n):", "    for i in range(0, min(len(seq), n), n):")
m("refresh writes: first batch only", R, "for i in range(0, len(ops), WRITE_BATCH):", "for i in range(0, min(len(ops), WRITE_BATCH), WRITE_BATCH):")
m("refresh deletes: first batch only", R, "for i in range(0, len(ids), schema.IN_BATCH):", "for i in range(0, min(len(ids), schema.IN_BATCH), schema.IN_BATCH):")
m("GET-404 branch removed", R, "    if resp.status == 404:\n        _mark_missing", "    if False:\n        _mark_missing")
m("stale_get removed", R, "    if pair == applied_pair and not doc.get('applying'):", "    if False:")
m("stale_get advances checked_at", R, "db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'missing': False}})",
  "db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'missing': False, 'checked_at': checked}})")
m("legacy gate on the GET removed", R, "    if kind == utils.STORAGE_LEGACY:\n", "    if False:\n")
m("apply does not clear missing", R, "                'missing': False,\n            },\n        },\n    )", "            },\n        },\n    )")
m("validators not re-applied", SC, "            db.command('collMod', name, validator=validator, validationLevel='strict')", "            pass")
m("no d index", SC, "    db[ENTRIES].create_index([('d', 1)], name='d_1')", "    pass")
m("fetch threads forced to 1", S, "max_workers=max(1, min(self._threads, len(tasks)))", "max_workers=1")
m("object_reader default path broken", S, "http_session, db_url = remote.create_s3_read_session(db_url=db_url, threads=threads)", "http_session, db_url = None, db_url")
m("_mark_missing without setOnInsert", R, "            '$setOnInsert': {'rev': 0, 'applied': None},\n", "")
m("metadata timestamp edge", S, "if include_timestamp and self._meta is not None:", "if include_timestamp:")
m("ensure_schema accepts bool", SC, "isinstance(chunk_ttl_seconds, bool) or ", "")
json.dump(M, open('mutants_review.json', 'w'), indent=1)
print(len(M))
# block compressors (2026-11-24)
M_EXTRA = [
    ("chunks compressed with zstd", SC, "COMPRESSORS = {DATASETS: 'zstd', ENTRIES: 'zstd', CHUNKS: 'none'}",
     "COMPRESSORS = {DATASETS: 'zstd', ENTRIES: 'zstd', CHUNKS: 'zstd'}"),
    ("compressor mismatch not warned", SC, "            if actual is not None and actual != wanted:", "            if False:"),
    ("new collections get no compressor", SC,
     "                storageEngine={'wiredTiger': {'configString': f'block_compressor={wanted}'}},\n", ""),
]
for name, f, old, new in M_EXTRA:
    m(name, f, old, new)
json.dump(M, open('mutants_review.json', 'w'), indent=1)
print(len(M))
# the ebooklet type (2026-11-24)
for name, f, old, new in [
    ("ebooklet type not recorded", R, "                'ebooklet_type': fields['type'],\n", ""),
    ("ebooklet type not exposed", S, "        self.ebooklet_type = doc.get('ebooklet_type')", "        self.ebooklet_type = None"),
]:
    m(name, f, old, new)
json.dump(M, open('mutants_review.json', 'w'), indent=1)
print(len(M))
