# Code review, round 2: ebooklet 0.11 write-order groups, the changes since round 1

## Framing

I am the author of the code below and I am reviewing my own work before it is released. I want an
independent critique on two axes, with equal weight:

1. **Accuracy:** does the code do what it claims, on every path, without losing or corrupting data?
2. **Simplicity:** can it be made simpler without losing a guarantee? Name what to cut, merge or
   rewrite, and say what (if anything) would be lost. "This is more code than the problem needs" is a
   finding.

Be direct. If something is wrong, say so and show why.

**Background.** ebooklet (scope `ebooklet`) syncs a local key/value file (the `booklet` package, from
PyPI) to S3-compatible storage. Version 0.11 replaces hash grouping with groups the WRITER assigns at push
time, in local-file write order, filled to a byte target (`group_bytes`, default 32 MiB). Each key's gid is
stored in its 19-byte remote-index entry (remote format 3); per-key storage (format 2) is unchanged.

**Round 1 already ran.** Code review `ebooklet-wog-code-1` (three arms) reviewed the first implementation.
Its records are in `ebooklet/planning/write-order-groups-code-review-*.md` (brief, three arm reports,
verdicts, synthesis). This round reviews **what changed after it**: the fixes it led to, three small
pre-existing bugs fixed since, and a design change made after a benchmark. Read the round-1 records for
context, but do not re-review settled decisions:
- the journal stays v1 for per-key files and v2 for grouped ones;
- cfdb and envlib keep their `_NOT_GIVEN` sentinels;
- the `num_groups` shim stays for one release.

The downstream scopes are cfdb (`cfdb`, its package directory), envlib (`envlib`, its package directory)
and envlib-ingest-base (`envlib-ingest-base`, the whole repo). This is ordinary data tooling; nothing here
targets a third party.

## What changed since round 1

There is no git commit at the round-1 state. The whole 0.11 change is `git diff HEAD` plus the untracked
files in the `ebooklet` scope. The items below are anchored to functions so you can find them. The
rationale for each is in the "Code review `ebooklet-wog-code-1`" table, and in the "Decision changed after
the benchmark" section at the end of `ebooklet/planning/write-order-groups-plan.md`.

| # | Change | Where |
|---|---|---|
| A1 | After the commit, `update_remote` calls `remote_session._load_db_metadata()`. The earlier `_note_commit` and the two post-push metadata reloads in `Change.push` are gone. | `utils.update_remote` (commit section), `main.Change.push`, `remote.py` |
| A2 | A push to an ABSENT remote (`not remote_session.initialized`) records every changelog key as a journaled write, and persists the journal, before any upload. | top of `utils.update_remote` |
| A3 | `hydrate_and_delete.py` (runs under ebooklet 0.10.5). It compares against a fresh GET of the db object. Backup and delete run inside the checked, locked session, and a remote 0.10 cannot read gets a clean ABORT. | `planning/write-order-groups-migration/` |
| B1 | `group_bytes` and every argument after it are keyword-only: `open_ebooklet`, `open_rcg`, both class constructors, cfdb `open_edataset`. | `main.py`, cfdb `edataset.py` |
| B2 | New tests: tail size at updated lengths, pull-time re-fetch, pull-time legacy refusal, journal write on mode adoption. The sidecar layout rule lost its `index_fetch_suppressed` clause, which I believe is implied by `remote_session.initialized`. | tests; `main._init_common` |
| B3 | `plan_groups` returns only the assignment. | `utils.plan_groups` |
| C1 | Dead delete machinery removed: `committed_delete_keys`, the `key in deletes` filter, the commit-time `journal.set_storage`, and `STORAGE_LEGACY_HASH` (a legacy int `num_groups` in a journal now maps to grouped). The empty-member-group branch became an `assert`. | `utils.update_remote`, `journal.py` |
| C2 | `StaleIndexError` removed. The stale-index check raises a plain internal `RuntimeError`. | `utils.update_remote`, `errors.py`, `__init__.py` |
| D1 | `update_remote` returns `(committed, failures)`. `PushResult.updated` is now whether the commit happened; before, it was True when every upload failed and nothing was committed. | `utils.update_remote`, `main.Change.push`, `PushResult` docstring |
| D2 | The open path stamps the local file with the remote's timestamp after it ingests a fetched index, last, as `_pull_remote_index` does. Before, readers re-downloaded the whole db object on every open after one remote change. | end of the remote section of `main._init_common` |
| D3 | A reader with no remote to ask (`flag == 'r'` and `storage_kind is None`: offline, or the remote is gone) takes its storage mode from its sidecar's value length. An explicit `group_bytes` for the other mode warns and loses, as "the remote wins" does. The sidecar layout rule then lost its "keep the sidecar offline" exception, which I believe is now unreachable. | `main._init_common`, the storage-resolution block |
| G | **Recorded `group_bytes`.** Each grouped commit writes `group_bytes` into the db-object metadata; per-key commits do not. `_load_db_metadata` parses it into `remote_session.group_bytes`. The target resolves as: explicit argument, then the remote's recorded value, then `DEFAULT_GROUP_BYTES` (`main._grouped_target`). This happens at open and on every `_pull_remote_index`. Readers report it as `db.group_bytes`. A writer that passes another value overwrites it, with no history kept. | `utils.update_remote` (commit metadata), `remote.py`, `main.py` |

Docs and changelogs were updated to match (ebooklet `README.md`, `CHANGELOG.md`, `docs/ops.md`,
`CLAUDE.md`; cfdb's docstrings; envlib's `publish` docstring). Claims in them are in scope.

## How I would like you to work

- **Check the claims, do not take them.** The changelog, docstrings and plan notes make many claims.
  Re-run anything a conclusion of yours depends on.
- **Execution means running the code under review.** Build the `ebooklet` scope's environment and drive the
  real `open_ebooklet` / `open_rcg` / push / read paths against the in-repo fake S3
  (`ebooklet/tests/fake_s3.py`: `FakeS3Connection(store, db_key)`), and run the test suite. A
  re-implementation of the logic in a script of your own is a model of your reading, not a test of the
  code; claims from one are weighted as reasoning. Paste the commands and their output.
- **Mutate to test the tests.** For any guard or test you doubt, change the code in your working copy and
  show whether a test goes red. Run mutants with `PYTHONDONTWRITEBYTECODE=1`, purge `__pycache__`, and
  restore the file afterwards. A test that cannot fail is a finding.
- **Evidence for every experiment.** For each experiment a finding rests on, paste its command and raw
  output into your reply, whoever ran it.
- **Look for what I did not think to check.** The questions below are where *I* suspect I am weak, which is
  exactly the wrong place to stop. Interactions between the changes above (for example A1 with G, A2 with
  D1, D2 with the reconciliation) are as much in scope as each change alone.
- **Separate severity.** "Wrong, will lose or corrupt data" is a different class from "I would write this
  differently", and both differ from "this can be cut".
- Do not open git-ignored local machine configuration (credentials files and the like); it is out of scope.

## Reproduction notes

The fake-S3 suites. The live-S3 files need credentials that are not shipped, so skip `test_ebooklet.py`,
`test_map.py`, `test_push_integrity.py`, `test_404_integrity.py`, `test_delete_safety_live.py`,
`test_rcg_entries.py`, `test_scale_push.py` and `utest_ebooklet.py`; their changed parts are still in
scope for reading.

```
cd /scratch/work/ebooklet
uv run python -m pytest -q -p no:cacheprovider -n 4 \
  ebooklet/tests/test_api_phase2.py ebooklet/tests/test_delete_safety.py ebooklet/tests/test_fsck.py \
  ebooklet/tests/test_generational.py ebooklet/tests/test_journal.py ebooklet/tests/test_member_integrity.py \
  ebooklet/tests/test_push_pipeline.py ebooklet/tests/test_remote_delete_reconcile.py \
  ebooklet/tests/test_stale_incarnation.py ebooklet/tests/test_utils.py ebooklet/tests/test_warnings_meta.py \
  ebooklet/tests/test_write_order_groups.py
# observed: 218 passed
```

**The pre-0.11 client** is on PyPI (`ebooklet==0.10.5`) and runs with
`uv run --no-project --with 'ebooklet==0.10.5' python <script>`. It does not ship its tests; load the repo's
`fake_s3.py` by path (it imports `ebooklet.remote`, which then resolves to 0.10.5).

**The migration script's harness** is `planning/write-order-groups-migration/t_hyd.py` (and `t_hyd_rcg.py`),
run under 0.10.5 from a scratch directory. Its paths assume `~/git/ebooklet`; point them at your copy. My run
(the eight cases plus the catalogue case) is described in the round-1 table. Case 7 (a stale local sidecar)
aborted with the fixed script and deleted the remote with the old comparison swapped back in.

**My mutation run:** fake suites, a fresh copy per mutant, `PYTHONDONTWRITEBYTECODE=1`. 44 mutants, 43
killed. The new ones cover:
- A1: no reload; A2: no journaling;
- B1: positional `group_bytes`, twice;
- D1: `updated` not equal to `committed`; D2: no open stamp;
- D3: no inference; writers infer too; the explicit argument beats the reader's index;
- G: no record; per-key records; load ignores the value; resolution ignores it; the recorded value beats
  an explicit one; pull keeps the default;
- the B2 pins.

The survivor is `copy_remote` branching on the manifest rather than the storage kind, which no reachable
state distinguishes. Treat all of this as a claim.

## Questions I am least confident about

These are starting points, not boundaries.

1. **A1.** `_load_db_metadata()` is a HEAD after the commit PUT, before the local stamp. What happens if it
   fails or returns stale or eventually-consistent metadata? Does reloading the session mid-push (format,
   `uuid`, `group_bytes`, `legacy_num_groups`) change behaviour for the GC phase that follows, or for a
   second push in the same session? Per-key and grouped both.
2. **A2.** Journaling every changelog key when the remote is absent:
   - the cost for a large hydrated file (the journal is a JSON blob in a reserved booklet slot);
   - the interplay with flag `'n'` / `replace_pending`, with the pre-push re-check that adopts a remote
     that appeared (that push is then no longer "absent"), and with a crash before the journal persist;
   - does writing the reserved slot during a push disturb the captured `loc_map` offsets or the
     compaction check?
3. **D1.** `Change.push` now unlinks the changelog only when `committed`. Any path where an uncommitted,
   failure-free push leaves state that the next push mishandles?
4. **D2.** The open path now claims freshness after ingesting an index. If the reconciliation scan was
   skipped (it gives up after two concurrent-write aborts), the stamp now prevents a re-run until the next
   remote change. Is that acceptable? Also check:
   - flag `'n'`, the index-fetch-suppressed path, `forget_remote_incarnation` (which sets the stamp to 0);
   - crash ordering;
   - whether anything else reads the stamp (`check_local_remote_sync` is the only reader I found).
5. **D3 and the layout rule.**
   - Can any session now discard a sidecar it should keep: offline, a reader whose remote is gone, a writer
     against an absent remote holding a legacy 15-byte sidecar?
   - Is "a 15-byte sidecar means per-key" ever wrong for a READER? A 0.10 hash-grouped sidecar is also 15
     bytes.
6. **G, the recorded `group_bytes`.**
   - Precedence: the explicit argument, then the remote's value, then the default.
   - A non-explicit writer adopts the remote's value on every pull, so another writer's change applies
     mid-session. Intended, but check it cannot split one push.
   - `copy_remote` carries the metadata (both copy paths).
   - `delete_remote` resets the field.
   - The value is NOT validated when loaded. I believe only ebooklet writes it, but a malformed value would
     make `int()` raise in `_load_db_metadata` for every open, readers included, and `0` would reach
     `plan_groups`. Is that a real risk worth a guard?
7. **A3.** Are the migration script's refusal conditions sufficient now? Does the in-session delete
   (`eb.delete_remote()` under 0.10.5, which keeps `remote_state.remote_ts` as a watermark) leave the local
   file in a state the 0.11 republish handles? The republish tests use a raw session delete instead.
8. **Tests.** Can any of the new tests pass vacuously? Look especially at:
   - `test_republish_*` (they rely on a fixture made by 0.10.5);
   - `test_open_fetches_a_changed_remote_once`;
   - `test_the_remote_records_group_bytes_and_later_writers_inherit_it`;
   - `test_pull_refuses_a_remote_that_became_legacy`.
9. **Simplicity.** Is any of the post-round-1 code more complicated than the problem needs? For example:
   - the `deciding` / warning block in the storage resolution;
   - `_grouped_target`;
   - `committed` next to `updated`;
   - the A2 journaling versus an alternative such as clearing `remote_state.remote_ts` when the remote is
     absent.

## Deliverable

A ranked list of findings, most serious first, tagged **accuracy** or **simplicity**. For each: what it is,
why it matters, your confidence, and the evidence (the file and line, or the command and its actual
output).

For every finding you mark verified, state:
- **(a)** what you executed or derived;
- **(b)** what it depends on that you did *not* check: assumptions inherited from me, baselines taken on
  trust, alternatives not tried. Clause (b) is the one I need most.

For simplicity findings, say what the simpler version is and what, if anything, it gives up.

Do not report a version, a timing or an output you did not observe; "I reasoned this from the source" is a
good answer, and invented provenance is worse than none. Refuting one of my claims with evidence is a
valuable outcome. If part of the code is sound, say what you checked to conclude that, not that it looks
reasonable.

---

# Appendix: the cfdb and envlib diffs (current tree against `HEAD`; their scopes have no git history)

### cfdb (scope `cfdb`; plus the new file `cfdb/tests/test_edataset_args.py`)
```diff
diff --git a/cfdb/__init__.py b/cfdb/__init__.py
index f8edd76..653bac1 100644
--- a/cfdb/__init__.py
+++ b/cfdb/__init__.py
@@ -13,4 +13,4 @@ try:
 except (ImportError, AttributeError):
     pass
 
-__version__ = '0.10.0'
+__version__ = '0.11.0'
diff --git a/cfdb/edataset.py b/cfdb/edataset.py
index 9b31a2f..742f342 100644
--- a/cfdb/edataset.py
+++ b/cfdb/edataset.py
@@ -14,6 +14,11 @@ import pathlib
 from . import utils
 from .main import Dataset
 
+## "Argument not given" for open_edataset's storage-mode arguments: only given
+## arguments are forwarded, so ebooklet's own defaults (inherit the remote's
+## mode and recorded group_bytes; grouped for a new dataset) apply otherwise.
+_NOT_GIVEN = object()
+
 
 class EDataset(Dataset):
     """
@@ -86,9 +91,11 @@ def open_edataset(remote_conn: Union[ebooklet.S3Connection, str, dict],
                   dataset_type: str='grid',
                   compression: str=utils.default_compression,
                   compression_level: int=None,
-                  num_groups: int = None,
+                  *,
+                  group_bytes=_NOT_GIVEN,
                   lock_timeout: int = 300,
                   force_lock: bool = False,
+                  num_groups=_NOT_GIVEN,
                   **kwargs):
     """
     Open a cfdb that is linked with a remote S3 database.
@@ -119,13 +126,15 @@ def open_edataset(remote_conn: Union[ebooklet.S3Connection, str, dict],
         The compression for all chunks, used only when a NEW dataset is created: attaching to an existing local or remote dataset always uses the compression it recorded, whatever is passed here. One of ``'zstd_shuffle'`` (default), ``'zstd'``, ``'lz4_shuffle'`` or ``'lz4'``; see ``open_dataset``.
     compression_level : int or None
         The compression level. None uses the defaults, which is 1 for every compression option.
-    num_groups : int or None
-        The number of groups for grouped S3 object storage. Required when creating a new database (flag='n'). For existing databases, this value is read from S3 metadata and the user-provided value is ignored.
-        Guidance: aim for groups of 10-100MB each. A reasonable starting point is max(10, total_expected_keys // 50). Too few groups means large S3 objects and slow partial updates; too many means more API calls per push. Each group's data is limited to 4GB due to offset encoding.
+    group_bytes : int, None, or omitted
+        The remote storage mode (ebooklet >= 0.11). An int: grouped storage - at each push the chunks new to the remote are packed, in the order they were written, into group objects of up to group_bytes bytes, so appending to a dataset uploads only the new chunks (plus at most one partly filled group). None: per-key storage, one remote object per chunk.
+        Omitted: an existing remote keeps its mode and the group_bytes it records (whatever its last push packed with); a NEW dataset is grouped at ebooklet.DEFAULT_GROUP_BYTES (32 MiB). Passing another int packs new chunks to it from then on and is recorded in place of the old value. Writing chunks in the order they will usually be read together (e.g. time band by time band) keeps them in the same groups. See ebooklet's "Grouped Storage".
     lock_timeout : int
         Maximum time in seconds to wait for the write lock when opening for write. Default is 300 (5 minutes). Only applies when flag is not ``'r'``. Raises ``TimeoutError`` if the lock cannot be acquired within the timeout.
     force_lock : bool
         If True, break any existing write locks before acquiring. Use this to recover from stale locks left by crashed processes. Default is False.
+    num_groups : None
+        Removed (hash grouping, ebooklet < 0.11). Forwarded to ebooklet for one release: an explicit num_groups=None keeps its old meaning, per-key storage; an int raises ValueError - use group_bytes.
     **kwargs
         Any kwargs that can be passed to ``ebooklet.open_ebooklet``.
 
@@ -135,9 +144,16 @@ def open_edataset(remote_conn: Union[ebooklet.S3Connection, str, dict],
     """
     if 'n_buckets' not in kwargs:
         kwargs['n_buckets'] = utils.default_n_buckets
+    ## Forward the storage-mode arguments only when given: omitted, ebooklet
+    ## inherits an existing remote's mode and recorded group_bytes (and
+    ## defaults a new one to grouped).
+    if group_bytes is not _NOT_GIVEN:
+        kwargs['group_bytes'] = group_bytes
+    if num_groups is not _NOT_GIVEN:
+        kwargs['num_groups'] = num_groups
 
     fp = pathlib.Path(file_path)
-    open_blt = ebooklet.open_ebooklet(remote_conn, file_path, flag, num_groups=num_groups, lock_timeout=lock_timeout, force_lock=force_lock, **kwargs)
+    open_blt = ebooklet.open_ebooklet(remote_conn, file_path, flag, lock_timeout=lock_timeout, force_lock=force_lock, **kwargs)
 
     try:
         # Create only when no dataset exists anywhere: get_metadata() transparently checks the remote as well as the local file, so a fresh local file attaches to an existing remote dataset instead of silently creating a new (empty) one over it. flag 'n' always creates new.
diff --git a/cfdb/tests/test_edataset.py b/cfdb/tests/test_edataset.py
index 8841450..157dda2 100644
--- a/cfdb/tests/test_edataset.py
+++ b/cfdb/tests/test_edataset.py
@@ -22,7 +22,7 @@ file_path = script_path.joinpath('test_remote.cfdb')
 name = 'air_temp'
 coords = ('latitude', 'longitude', 'time')
 chunk_shape = (20, 30, 10)
-num_groups = 10
+group_bytes = 2**16   # small groups, so a test dataset spans several
 
 sel = (slice(1, 4), slice(None, None), slice(2, 5))
 loc_sel = (slice(0.4, 0.7), slice(None, None), slice('1970-01-04', '1970-01-10'))
@@ -113,7 +113,7 @@ def pushed_dataset(remote_conn):
         data_var = ds.create.data_var.generic(name, coords, data_dtype, chunk_shape=chunk_shape)
         data_var[:] = data_var_data
 
-    with open_edataset(remote_conn, file_path, flag='w', num_groups=num_groups) as ds:
+    with open_edataset(remote_conn, file_path, flag='w', group_bytes=group_bytes) as ds:
         changes = ds.changes()
         assert changes.push()
 
@@ -339,7 +339,7 @@ def test_edataset_midsession_push(fg1_conn):
     """
     _clean_fg_local()
 
-    with open_edataset(fg1_conn, fg1_file_path, flag='n', num_groups=num_groups) as ds:
+    with open_edataset(fg1_conn, fg1_file_path, flag='n', group_bytes=group_bytes) as ds:
         ds.create.coord.lat(data=lat_data, chunk_shape=(20,))
         ds.attrs['project'] = 'footgun-test'
         ds['latitude'].attrs['note'] = 'mid-session'
@@ -373,14 +373,14 @@ def fg2_pushed(fg2_conn):
     """Create + push + close a small dataset for the attach regressions."""
     _clean_fg_local()
 
-    with open_edataset(fg2_conn, fg2_file_path, flag='n', num_groups=num_groups) as ds:
+    with open_edataset(fg2_conn, fg2_file_path, flag='n', group_bytes=group_bytes) as ds:
         ds.create.coord.lat(data=lat_data, chunk_shape=(20,))
         dv = ds.create.data_var.generic('temp', ('latitude',), dtypes.dtype('float32'), chunk_shape=(20,))
         dv[:] = lat_data
         ds.attrs['origin'] = 'fg2'
 
-    # num_groups must be re-passed here: the remote doesn't exist yet, so it can't be read from S3 metadata (same pattern as pushed_dataset above)
-    with open_edataset(fg2_conn, fg2_file_path, flag='w', num_groups=num_groups) as ds:
+    # group_bytes re-passed: the remote doesn't exist yet (the journal records the grouped mode; the byte target is a writer setting)
+    with open_edataset(fg2_conn, fg2_file_path, flag='w', group_bytes=group_bytes) as ds:
         assert ds.push()
 
     _clean_fg_local()
@@ -489,7 +489,7 @@ def test_edataset_ts_ortho_e2e(ts1_conn, fg2_pushed):
     geo_data = [shapely.Point(x, y) for x, y in zip(np.linspace(-5, 4.9, 20), np.linspace(0, 9.9, 20))]
     ts_values = np.linspace(0, 199.9, 200, dtype='float32').reshape(20, 10)
 
-    with open_edataset(ts1_conn, ts1_file_path, flag='n', dataset_type='ts_ortho', num_groups=num_groups) as ds:
+    with open_edataset(ts1_conn, ts1_file_path, flag='n', dataset_type='ts_ortho', group_bytes=group_bytes) as ds:
         assert type(ds).__name__ == 'ETimeSeriesOrtho'
         assert ds.dataset_type == 'ts_ortho'
         geo_coord = ds.create.coord.point()
@@ -498,8 +498,8 @@ def test_edataset_ts_ortho_e2e(ts1_conn, fg2_pushed):
         dv = ds.create.data_var.generic('temp', ('point', 'time'), dtypes.dtype('float32'), chunk_shape=(10, 10))
         dv[:] = ts_values
 
-    # num_groups must be re-passed: the remote doesn't exist yet (same pattern as fg2_pushed)
-    with open_edataset(ts1_conn, ts1_file_path, flag='w', num_groups=num_groups) as ds:
+    # group_bytes re-passed: the remote doesn't exist yet (same pattern as fg2_pushed)
+    with open_edataset(ts1_conn, ts1_file_path, flag='w', group_bytes=group_bytes) as ds:
         assert type(ds).__name__ == 'ETimeSeriesOrtho'
         assert ds.push()
 
```

### envlib (scope `envlib`)
```diff
diff --git a/envlib/__init__.py b/envlib/__init__.py
index 6666111..a654974 100644
--- a/envlib/__init__.py
+++ b/envlib/__init__.py
@@ -4,7 +4,7 @@ from envlib import vocabularies
 from envlib.catalogue import Catalogue, DatasetRef, validate_dataset
 from envlib.metadata import Metadata, ValidationError, canonical_station_point, compute_station_id
 
-__version__ = '0.1.7'
+__version__ = '0.1.8'
 
 __all__ = [
     'Catalogue',
diff --git a/envlib/catalogue.py b/envlib/catalogue.py
index 9e684a6..b47d4af 100644
--- a/envlib/catalogue.py
+++ b/envlib/catalogue.py
@@ -42,6 +42,10 @@ DEFAULT_CACHE_DIR = '~/.envlib/cache'
 
 STATION_ID_VAR = 'station_id'
 
+# "Argument not given" for publish()'s group_bytes: only a given value is
+# forwarded, so ebooklet's own default applies otherwise.
+_NOT_GIVEN = object()
+
 # cfdb dataset types, grouped by the structure envlib cares about. Defined once because every
 # check below used exact string equality against a single value, which is how a new dataset type
 # silently skips a guard -- most dangerously _check_stations.
@@ -989,10 +993,17 @@ class Catalogue:
         return validate_dataset(local_cfdb_path)
 
     def publish(
-        self, local_cfdb_path, remote_conn, rcg_remote_conn, num_groups=None, verify_objects: bool = True, **open_kwargs
+        self, local_cfdb_path, remote_conn, rcg_remote_conn, group_bytes=_NOT_GIVEN, verify_objects: bool = True,
+        **open_kwargs
     ) -> dict:
         """Validate, push the cfdb data to its S3 remote, verify it, then register it in the RCG.
 
+        ``group_bytes`` sets how the remote stores chunks (ebooklet >= 0.11): an
+        int packs chunks into write-order groups of up to that many bytes, None
+        stores one object per chunk. Omitted, an existing remote keeps its mode
+        and recorded group_bytes, and a new one is grouped (ebooklet's
+        default, 32 MiB).
+
         The cfdb data is pushed BEFORE the RCG entry so the catalogue never
         references incomplete remote data. With ``verify_objects`` (default True),
         after the push and before the entry is written the remote is fsck'd, and a
@@ -1004,8 +1015,8 @@ class Catalogue:
         """
         member_conn = _as_connection(remote_conn)
         edataset_kwargs = dict(open_kwargs)
-        if num_groups is not None:
-            edataset_kwargs['num_groups'] = num_groups
+        if group_bytes is not _NOT_GIVEN:
+            edataset_kwargs['group_bytes'] = group_bytes
         # validate INSIDE the edataset session: for a re-publish of an
         # already-pushed (possibly partially materialized) local file, plain
         # open_dataset would read local chunks only and could extract wrong
diff --git a/envlib/tests/test_catalogue_live.py b/envlib/tests/test_catalogue_live.py
index 036ccae..7c00e1c 100644
--- a/envlib/tests/test_catalogue_live.py
+++ b/envlib/tests/test_catalogue_live.py
@@ -71,7 +71,7 @@ def test_publish_query_open_roundtrip(live, s3_config, tmp_path, cache_dir):
     with pytest.warns(UserWarning, match='treating as empty'):
         publisher = Catalogue(remotes=[rcg_conn], cache=str(cache_dir))
     assert publisher.datasets == []
-    result = publisher.publish(local, data_conn, rcg_conn, num_groups=11)
+    result = publisher.publish(local, data_conn, rcg_conn, group_bytes=2**16)
     assert result['dataset_version_id'] == meta.dataset_version_id
 
     # a fresh consumer catalogue sees the entry
@@ -105,7 +105,7 @@ def test_republish_noop_keeps_modified_at(live, tmp_path, cache_dir):
 
     with pytest.warns(UserWarning, match='treating as empty'):
         cat = Catalogue(remotes=[rcg_conn], cache=str(cache_dir))
-    cat.publish(local, data_conn, rcg_conn, num_groups=11)
+    cat.publish(local, data_conn, rcg_conn, group_bytes=2**16)
     first = cat.query(variable='temperature')[0].metadata
 
     cat.publish(local, data_conn, rcg_conn)
@@ -130,7 +130,7 @@ def test_ts_ortho_publish_roundtrip(live, s3_config, tmp_path, cache_dir):
     meta = build_ts(local)
 
     cat = Catalogue(remotes=[], cache=str(cache_dir))
-    cat.publish(local, data_conn, rcg_conn, num_groups=11)
+    cat.publish(local, data_conn, rcg_conn, group_bytes=2**16)
 
     consumer = _catalogue(rcg_conn, cache_dir / 'consumer')
     refs = consumer.query(dataset_type='ts_ortho')
@@ -162,7 +162,7 @@ def test_register_existing_remote(live, tmp_path, cache_dir):
     meta = build_grid(local)
 
     # push the cfdb outside envlib (the pipeline-managed case)
-    with cfdb.open_edataset(data_conn, local, flag='w', num_groups=11) as eds:
+    with cfdb.open_edataset(data_conn, local, flag='w', group_bytes=2**16) as eds:
         eds.push()
 
     with pytest.warns(UserWarning, match='treating as empty'):
@@ -185,7 +185,7 @@ def test_deregister_guard_and_delete(live, s3_config, tmp_path, cache_dir):
 
     with pytest.warns(UserWarning, match='treating as empty'):
         cat = Catalogue(remotes=[rcg_conn], cache=str(cache_dir))
-    cat.publish(local, data_conn, rcg_conn, num_groups=11)
+    cat.publish(local, data_conn, rcg_conn, group_bytes=2**16)
 
     # the real shared-target trap: fix/bump the SAME file's identity (the
     # typo-correction flow: clear the stale self-identification attrs, change
@@ -237,6 +237,6 @@ def test_deregister_missing_entry_raises(live, cache_dir, tmp_path):
         cat = Catalogue(remotes=[rcg_conn], cache=str(cache_dir))
     # seed the RCG so it exists remotely
     data_conn = live('missing/data.cfdb')
-    cat.publish(local, data_conn, rcg_conn, num_groups=11)
+    cat.publish(local, data_conn, rcg_conn, group_bytes=2**16)
     with pytest.raises(ValidationError, match='no catalogue entry'):
         cat.deregister('0' * 24, rcg_conn)
```
