import json
T = ["ebooklet/tests/test_mongo_refresh.py", "ebooklet/tests/test_mongo_store.py", "ebooklet/tests/test_mongo_cfdb.py"]
R, S, SC = "ebooklet/mongo/refresh.py", "ebooklet/mongo/store.py", "ebooklet/mongo/schema.py"
M = []
def m(name, f, old, new): M.append({"name": name, "file": f, "old": old, "new": new, "tests": T})
m("return None on a fetch failure", S,
  "        fresh = self._read_entries(list(failures))",
  "        for k in failures:\n            self._values[k] = None\n        return\n        fresh = self._read_entries(list(failures))")
m("skip the rev check on an absent entry", S, "if keys and self._changed():", "if False and keys and self._changed():")
m("no rev increment in step 2", R, "'$inc': {'rev': 1},", "'$inc': {'rev': 0},")
m("metadata before entries", R,
  "    ## Apply step 1: upsert changed and new entries.\n",
  "    db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'meta_section': Binary(bytes(meta_section)) if meta_section else None}}, upsert=True)\n    ## Apply step 1: upsert changed and new entries.\n")
m("applied before the deletes", R,
  "    ## Apply step 3: delete the removed entries.\n",
  "    db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'applied': {'uuid': uuid_hex, 'ts': fields['timestamp']}}})\n    ## Apply step 3: delete the removed entries.\n")
m("no rev condition in step 4", R, "{'_id': dataset_id, 'rev': rev},", "{'_id': dataset_id},")
m("GET skip on newer instead of equal", R, "    if pair == applied_pair and not doc.get('applying'):\n", "    if applied_pair and pair[1] <= applied_pair[1] and not doc.get('applying'):\n")
m("HEAD nochange on newer instead of equal", R,
  "    if applied_pair == (session.uuid.hex, session.timestamp) and not doc.get('applying'):\n",
  "    if applied_pair and session.timestamp <= applied_pair[1] and not doc.get('applying'):\n")
m("diff on ts only", R, "changed = [k for k, e in new.items() if cur.get(k) != e]",
  "changed = [k for k, e in new.items() if (cur.get(k) or {}).get('ts') != e['ts']]")
m("diff without gen", R, "changed = [k for k, e in new.items() if cur.get(k) != e]",
  "changed = [k for k, e in new.items() if {**(cur.get(k) or {}), 'n': 0} != {**e, 'n': 0}]")
m("per-key label = index ts", S, "chunk_id(self._d, entry['u'], k, ts)", "chunk_id(self._d, entry['u'], k, entry['ts'])")
m("no uuid in the chunk id", SC, "return f'{dataset_id}|{uuid_hex}|{key}|{ts}'", "return f'{dataset_id}|{key}|{ts}'")
m("compare with a freshly read applied", S,
  "        if (fields['uuid'].hex, fields['timestamp']) == self._applied:",
  "        _a = self._db[DATASETS].find_one({'_id': self._d})['applied']\n        if (fields['uuid'].hex, fields['timestamp']) == ((_a['uuid'], _a['ts']) if _a else None):")
m("no refetch before a fault", S,
  "        items, failures = self._fetch(retry)\n        self._store(retry, items)\n        if not failures:\n            return\n\n        ## 3.",
  "        failures = retry\n\n        ## 3.")
m("invert the HEAD classification", S, "        if (fields['uuid'].hex, fields['timestamp']) == self._applied:",
  "        if (fields['uuid'].hex, fields['timestamp']) != self._applied:")
m("do not clear missing", R, "{'$set': {'missing': False, 'checked_at': checked}}", "{'$set': {'checked_at': checked}}")
m("checked_at on a failed refresh", R, "    checked = _now()\n",
  "    checked = _now()\n    db[DATASETS].update_one({'_id': dataset_id}, {'$set': {'checked_at': checked}})\n")
m("get bypasses the memo", S, "        if key not in self._values:\n            self._fill([key])", "        if True:\n            self._fill([key])")
m("store ignores missing", S, "        if doc.get('missing'):", "        if False:")
m("reader sends the HEAD at construction", S, "        self.threads = threads\n\n\ndef object_reader",
  "        self.threads = threads\n        self._load_db_metadata()\n\n\ndef object_reader")
m("fetch an unmanifested group", S, "            elif e['n'] is None:", "            elif False:")
json.dump(M, open('mutants_mongo.json', 'w'), indent=1)
print(len(M))
