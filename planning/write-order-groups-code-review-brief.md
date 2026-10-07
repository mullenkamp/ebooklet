# Code review — ebooklet 0.11: write-order groups (remote format 3)

## Framing

I am the author of the code below and I am reviewing my own work before it is released. I want an
independent critique on two axes, with equal weight:

1. **Accuracy:** does the code do what it claims, on every path, without losing or corrupting data?
2. **Simplicity:** can it be made simpler without losing a guarantee? Name what to cut, merge or
   rewrite, and say what (if anything) would be lost. "This is more code than the problem needs" is a
   finding.

Be direct; if something is wrong, say so and show why.

ebooklet (scope `ebooklet`) syncs a local key/value file (the `booklet` package, from PyPI) to
S3-compatible storage. Until now, grouped storage assigned keys to group objects by
`blake2b(key) % num_groups`, so an append re-uploaded most groups. This change replaces that with groups
the WRITER assigns at push time, in local-file write order, filled to a byte target (`group_bytes`,
default 32 MiB). Each key's group id is stored in its 19-byte remote-index entry (remote format 3).
Per-key storage (format 2) is unchanged. Deletes in grouped mode are lazy (index-only). The change also
fixes two pre-existing bugs (a stale index after a crashed commit or a broken lock; a discard that lost a
key), and replaces the `num_groups` argument with `group_bytes` downstream: cfdb (scope `cfdb`, package
dir), envlib (scope `envlib`, package dir), envlib-ingest-base (scope `envlib-ingest-base`, whole repo).
This is ordinary data tooling; nothing here targets a third party.

Where to read:
- `ebooklet/planning/write-order-groups-plan.md`: the approved plan, including "Implementation notes
  (2026-10-07)" with deviations.
- `ebooklet/planning/write-order-groups-review-*.md`: the plan-review round and its verdicts (context
  only; do not re-review the plan).
- The change itself: in the `ebooklet` scope, `git diff HEAD` plus the untracked files
  (`git status`): `ebooklet/tests/test_write_order_groups.py`, `ebooklet/tests/groups.py`,
  `ebooklet/tests/fixtures/` (a fixture made by ebooklet 0.10.5, with its generator script), and
  `planning/write-order-groups-migration/hydrate_and_delete.py`. In `envlib-ingest-base`, `git diff HEAD`.
  The cfdb and envlib diffs are appended below (their scopes are package directories without git
  history).
- `ebooklet/CHANGELOG.md` (0.11.0) and `ebooklet/CLAUDE.md` ("Grouped S3 Storage") summarise the design.

## How I would like you to work

- **Check the claims, do not take them.** The changelog, docstrings and plan notes make many claims;
  re-run anything a conclusion of yours depends on.
- **Execution means running the code under review.** Build the `ebooklet` scope's environment and drive
  the real `open_ebooklet` / `open_rcg` / push / read paths against the in-repo fake S3
  (`ebooklet/tests/fake_s3.py`: `FakeS3Connection(store, db_key)`), and run the test suite. A
  re-implementation of the logic in a script of your own is a model of your reading, not a test of the
  code; claims from one are weighted as reasoning. Paste the commands and their output.
- **Mutate to test the tests.** For any guard or test you doubt, change the code in your working copy
  and show whether a test goes red. Run mutants with `PYTHONDONTWRITEBYTECODE=1` and restore the file
  afterwards. A test that cannot fail is a finding.
- **Evidence for every experiment.** For each experiment a finding rests on, paste its command and raw
  output into your reply, whoever ran it.
- **Look for what I did not think to check.** The questions below are where *I* suspect I am weak,
  which is exactly the wrong place to stop.
- **Separate severity.** "Wrong, will lose or corrupt data" is a different class from "I would write
  this differently", and both differ from "this can be cut".
- Do not open git-ignored local machine configuration (credentials files and the like); out of scope.

## Reproduction notes

The fake-S3 suites (the live-S3 files need credentials that are not shipped; skip
`test_ebooklet.py`, `test_map.py`, `test_push_integrity.py`, `test_404_integrity.py`,
`test_delete_safety_live.py`, `test_rcg_entries.py`, `test_scale_push.py` and `utest_ebooklet.py`;
their changed parts are still in scope for reading):

```
cd /scratch/work/ebooklet
uv run python -m pytest -q -p no:cacheprovider -n 4 \
  ebooklet/tests/test_api_phase2.py ebooklet/tests/test_delete_safety.py ebooklet/tests/test_fsck.py \
  ebooklet/tests/test_generational.py ebooklet/tests/test_journal.py ebooklet/tests/test_member_integrity.py \
  ebooklet/tests/test_push_pipeline.py ebooklet/tests/test_remote_delete_reconcile.py \
  ebooklet/tests/test_stale_incarnation.py ebooklet/tests/test_utils.py ebooklet/tests/test_warnings_meta.py \
  ebooklet/tests/test_write_order_groups.py
# observed: 203 passed
```

The pre-0.11 client is on PyPI (`ebooklet==0.10.5`) and runs with
`uv run --no-project --with 'ebooklet==0.10.5' python <script>`. It does not ship its tests; load the
repo's `fake_s3.py` by path (it imports `ebooklet.remote`, which then resolves to 0.10.5). That is how the
fixture (`ebooklet/tests/fixtures/make_legacy_0105.py`) and the migration script were exercised. The
cross-version per-key read (a 0.11 per-key remote opened by 0.10.5) has NOT been run; it is a good
decisive experiment.

My mutation run (fake suites, a fresh copy per mutant): 25 mutants, 24 killed. The survivor is
`copy_remote` branching on the manifest rather than the storage kind, which I believe no reachable
state distinguishes. The list covered allocation order, tail top-up and its pull, the deletes commit
gate, the emptied-group drop, journal clearing for failed groups, `<` vs `<=`, oversized values, the
stale-index belt, the stamp order, the remote-state re-fetch, the format stamp, legacy classification,
journal version, the sidecar layout rule, the `group_bytes` sentinel, the discard fix, two fsck fields,
the GC error contract, the pull-time mode adoption and the `num_groups` shim. Treat that as a claim.

## Questions I am least confident about

These are starting points, not boundaries.

1. **Index/manifest consistency on every path.** The local file is now stamped last, and an open or
   pull re-fetches the whole index whenever the cached `remote_state.remote_ts` differs from the remote's
   timestamp. Can this re-fetch fire where it should not (e.g. after `forget_remote_incarnation`, which
   keeps `remote_ts` as a watermark), and does the reconciliation that follows a re-fetch ever delete a
   local value it should keep? Can a writer still commit from a stale index anywhere?
2. **Group membership from the index alone.** A repack's members are the index members of that gid plus
   this push's allocations. Check every path that edits the sidecar before or during a push (journal
   delete replay, lost-key drops, pulls that fail, the replacement purge, `clear()`, `delete_remote()`).
   Can a live value be dropped from a group, or a deleted one resurrected?
3. **Allocation and the tail.** `plan_groups` sizes entries with `group_entry_size`; does that match what
   `pack_group` writes? Is the tail's projected size right when the tail also has updated members, lost
   members or a failed pull? With failed fresh groups, can a gid number be reused within one push or in
   the retry in a way that confuses the manifest, the index or GC? Replacement pushes start at gid 0 -
   any collision with old objects before the sweep?
4. **Lazy deletes.** The deletes-only commit is owned by the commit gate. Are emptied groups detected and
   GC'd in every case, including when the only change is deletes, or when a group empties through a
   lost-key drop?
5. **Modes and compatibility.** The `group_bytes` sentinel and its inheritance rules, the `num_groups`
   shim, legacy classification (`storage_kind`), the per-mode format stamp, journal v2 only for grouped
   files, and the sidecar layout rule (discard online, keep offline). Is a per-key remote written by 0.11
   still fully usable by 0.10.5? Can any 0.11 path stamp format 2 with 19-byte entries, or format 3 with
   15-byte ones?
6. **The discard fix** forces an index re-pull whenever a discarded written key is missing from the
   sidecar. Any side effect of that re-pull (reconciliation, journal replay) that loses data?
7. **The migration script** (`hydrate_and_delete.py`): are its refusal conditions sufficient before it
   deletes a remote? Anything it can delete that it should not?
8. **Downstream.** cfdb forwards `group_bytes`/`num_groups` only when given; envlib `publish()` likewise;
   ingest-base's helpers now pass `group_bytes=None` on build and nothing on update. Correct, and is the
   new default (new datasets grouped) safe for every caller in scope?
9. **Simplicity.** Which parts could be removed or merged without losing a guarantee? Candidates I am
   unsure of: the `StaleIndexError` belt (redundant with the open-path fix?), the fsck fields
   (`bad_entry_layout`, `dead_fraction`), the `copy_remote` change, the journal's v1/v2 split, the
   sidecar layout rule, and the size of the new test file. Also look at the diff as a whole: is any of it
   more complicated than the problem needs?

## Deliverable

A ranked list of findings, most serious first, tagged **accuracy** or **simplicity**: what it is, why it
matters, your confidence, and the evidence (the file and line, or the command and its actual output).
For every finding you mark verified, state **(a)** what you executed or derived, and **(b)** what it
depends on that you did *not* check: assumptions inherited from me, baselines taken on trust,
alternatives not tried. Clause (b) is the one I need most. For simplicity findings, say what the simpler
version is and what, if anything, it gives up.

Do not report a version, a timing or an output you did not observe; "I reasoned this from the source" is
a good answer, invented provenance is worse than none. Refuting one of my claims with evidence is a
valuable outcome. If part of the code is sound, say what you checked to conclude that, not that it looks
reasonable.

---

# Appendix: the cfdb and envlib diffs

### cfdb (repo root ~/git/cfdb-repos/cfdb; scope `cfdb` = its package directory)
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
index 9b31a2f..d2507cc 100644
--- a/cfdb/edataset.py
+++ b/cfdb/edataset.py
@@ -14,6 +14,11 @@ import pathlib
 from . import utils
 from .main import Dataset
 
+## "Argument not given" for open_edataset's storage-mode arguments: only given
+## arguments are forwarded, so ebooklet's own defaults (inherit the remote's
+## mode; grouped for a new dataset) apply otherwise.
+_NOT_GIVEN = object()
+
 
 class EDataset(Dataset):
     """
@@ -86,9 +91,10 @@ def open_edataset(remote_conn: Union[ebooklet.S3Connection, str, dict],
                   dataset_type: str='grid',
                   compression: str=utils.default_compression,
                   compression_level: int=None,
-                  num_groups: int = None,
+                  group_bytes=_NOT_GIVEN,
                   lock_timeout: int = 300,
                   force_lock: bool = False,
+                  num_groups=_NOT_GIVEN,
                   **kwargs):
     """
     Open a cfdb that is linked with a remote S3 database.
@@ -119,13 +125,15 @@ def open_edataset(remote_conn: Union[ebooklet.S3Connection, str, dict],
         The compression for all chunks, used only when a NEW dataset is created: attaching to an existing local or remote dataset always uses the compression it recorded, whatever is passed here. One of ``'zstd_shuffle'`` (default), ``'zstd'``, ``'lz4_shuffle'`` or ``'lz4'``; see ``open_dataset``.
     compression_level : int or None
         The compression level. None uses the defaults, which is 1 for every compression option.
-    num_groups : int or None
-        The number of groups for grouped S3 object storage. Required when creating a new database (flag='n'). For existing databases, this value is read from S3 metadata and the user-provided value is ignored.
-        Guidance: aim for groups of 10-100MB each. A reasonable starting point is max(10, total_expected_keys // 50). Too few groups means large S3 objects and slow partial updates; too many means more API calls per push. Each group's data is limited to 4GB due to offset encoding.
+    group_bytes : int, None, or omitted
+        The remote storage mode (ebooklet >= 0.11). An int: grouped storage - at each push the chunks new to the remote are packed, in the order they were written, into group objects of up to group_bytes bytes, so appending to a dataset uploads only the new chunks (plus at most one partly filled group). None: per-key storage, one remote object per chunk.
+        Omitted: an existing remote keeps its mode; a NEW dataset is grouped at ebooklet.DEFAULT_GROUP_BYTES (32 MiB). Writing chunks in the order they will usually be read together (e.g. time band by time band) keeps them in the same groups. See ebooklet's "Grouped Storage".
     lock_timeout : int
         Maximum time in seconds to wait for the write lock when opening for write. Default is 300 (5 minutes). Only applies when flag is not ``'r'``. Raises ``TimeoutError`` if the lock cannot be acquired within the timeout.
     force_lock : bool
         If True, break any existing write locks before acquiring. Use this to recover from stale locks left by crashed processes. Default is False.
+    num_groups : None
+        Removed (hash grouping, ebooklet < 0.11). Forwarded to ebooklet for one release: an explicit num_groups=None keeps its old meaning, per-key storage; an int raises ValueError - use group_bytes.
     **kwargs
         Any kwargs that can be passed to ``ebooklet.open_ebooklet``.
 
@@ -135,9 +143,15 @@ def open_edataset(remote_conn: Union[ebooklet.S3Connection, str, dict],
     """
     if 'n_buckets' not in kwargs:
         kwargs['n_buckets'] = utils.default_n_buckets
+    ## Forward the storage-mode arguments only when given: omitted, ebooklet
+    ## inherits an existing remote's mode (and defaults a new one to grouped).
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
 
diff --git a/pyproject.toml b/pyproject.toml
index 176fca7..3ccba8b 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -44,7 +44,7 @@ dependencies = [
 
 [project.optional-dependencies]
 netcdf4 = ['h5netcdf']
-ebooklet = ['ebooklet>=0.10.0']
+ebooklet = ['ebooklet>=0.11.0']
 xarray = ['xarray']
 
 [project.entry-points."xarray.backends"]
@@ -60,13 +60,13 @@ dev = [
   "pytest-cov",
   'h5netcdf',
   'h5py>=3.6.0',
-  'ebooklet>=0.10.0',
+  'ebooklet>=0.11.0',
   'xarray',
 ]
 docs = [
   "mkdocs-material>=9.0",
   "mkdocstrings[python]>=0.24",
-  "ebooklet>=0.10.0",
+  "ebooklet>=0.11.0",
 ]
 
 [tool.hatch]
```
New file `cfdb/tests/test_edataset_args.py` (in scope).

### envlib (repo root ~/git/envlib-repos/envlib; scope `envlib` = its package directory)
```diff
diff --git a/CHANGELOG.md b/CHANGELOG.md
index 1a2250c..eef1e61 100644
--- a/CHANGELOG.md
+++ b/CHANGELOG.md
@@ -3,7 +3,17 @@
 Notable changes to envlib. The format loosely follows [Keep a Changelog](https://keepachangelog.com/);
 envlib does not promise SemVer before 1.0 — minor versions may change behavior.
 
-## 0.1.7 (unreleased)
+## 0.1.8 (unreleased)
+
+- **Requires cfdb >= 0.11.0 and ebooklet >= 0.11.0.** ebooklet 0.11 stores grouped remotes in
+  write-order groups (remote format 3), which older clients refuse ("upgrade ebooklet"); the commons
+  catalogue and its grouped datasets are republished in that format.
+- **`publish(..., group_bytes=...)` replaces `num_groups`.** An int packs a dataset's chunks into
+  write-order groups of up to that many bytes, so appending to a published dataset uploads only the
+  new chunks; `None` stores one object per chunk. Omitted, an existing remote keeps its layout and a
+  new one is grouped (ebooklet's default, 32 MiB).
+
+## 0.1.7
 
 - **Requires cfdb >= 0.10.0.** cfdb 0.10 compresses new datasets with a byte-shuffle filter
   (`zstd_shuffle`, its new default), and an older cfdb cannot open them: it fails at open with
diff --git a/docs/getting-started/quickstart.md b/docs/getting-started/quickstart.md
index d928b7c..4d1d5b7 100644
--- a/docs/getting-started/quickstart.md
+++ b/docs/getting-started/quickstart.md
@@ -116,7 +116,7 @@ rcg_conn = S3Connection(
 )
 
 cat = envlib.Catalogue(remotes=[rcg_conn])
-cat.publish('data.cfdb', data_conn, rcg_conn, num_groups=101)   # prime numbers hash best
+cat.publish('data.cfdb', data_conn, rcg_conn)   # grouped S3 layout by default (group_bytes)
 ```
 
 The data is pushed **before** the catalogue entry is written, so the catalogue never references incomplete data; re-running a failed publish is safe. Any consumer with the catalogue's location can now run the query at the top of this page and find your dataset.
diff --git a/docs/guide/publishing.md b/docs/guide/publishing.md
index e047bc2..d19cdd8 100644
--- a/docs/guide/publishing.md
+++ b/docs/guide/publishing.md
@@ -6,14 +6,14 @@
 
 ```python
 cat = envlib.Catalogue(remotes=[rcg_conn])
-cat.publish('era5_temp_v1.cfdb', data_conn, rcg_conn, num_groups=101)
+cat.publish('era5_temp_v1.cfdb', data_conn, rcg_conn)
 ```
 
 Internally, in order: validate → write the derived attributes into the file (`envlib_dataset_id`, `envlib_dataset_version_id`, and the auto-populated `standard_name`) → push the cfdb data → verify the pushed objects → write the catalogue entry → push the catalogue. The data goes up **before** the entry, so the catalogue never references incomplete data.
 
 - `data_conn` is where the dataset lives (an `ebooklet.S3Connection` with your credentials). Set its `db_url` to the dataset's public HTTPS location if you host it publicly — that URL rides into the entry as `data_url` so consumers can open the dataset credential-free. It must be a *plain* public URL: no `user:pass@`, no query string (presigned URLs are rejected — their signatures must never land in a catalogue).
 - `rcg_conn` is the catalogue's own S3 location. A catalogue that doesn't exist yet is created on first publish.
-- `num_groups` tunes the S3 object layout for a **new** remote dataset (see [cfdb's S3 guide](https://mullenkamp.github.io/cfdb/guide/s3-remote/)); it's ignored for existing ones. **It's best to use a prime number**.
+- `group_bytes` sets the S3 object layout: chunks are packed, in the order they were written, into group objects of up to that many bytes (default 32 MiB, the setting for most datasets, including ones that grow by appending); `group_bytes=None` stores one object per chunk, for datasets pushed very often in tiny increments. Omitted, an existing remote keeps its layout (see [cfdb's S3 guide](https://mullenkamp.github.io/cfdb/guide/s3-remote/)).
 
 **Failure handling & object verification**: if publish dies between the data push and the catalogue write, just run it again — the data push is idempotent and the entry write is an upsert. A *partial* push failure (some objects could not be transferred) raises a `RuntimeError` naming the failed keys rather than claiming success; the pending changes are retained, so fixing the cause and re-running completes it.
 
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
index 9e684a6..c5694ac 100644
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
@@ -989,10 +993,16 @@ class Catalogue:
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
+        and a new one is grouped (ebooklet's default, 32 MiB).
+
         The cfdb data is pushed BEFORE the RCG entry so the catalogue never
         references incomplete remote data. With ``verify_objects`` (default True),
         after the push and before the entry is written the remote is fsck'd, and a
@@ -1004,8 +1014,8 @@ class Catalogue:
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
diff --git a/pyproject.toml b/pyproject.toml
index 0a28a97..931e442 100644
--- a/pyproject.toml
+++ b/pyproject.toml
@@ -19,9 +19,12 @@ dependencies = [
   # >=0.10.0: cfdb 0.10 writes new datasets with byte-shuffled compression (zstd_shuffle), which an
   # older cfdb refuses at open with a bare "Invalid enum value 'zstd_shuffle'" naming no remedy.
   # Readers must be able to open anything a current producer publishes, so envlib floors the
-  # version that can. (It needs no other 0.10 feature.)
-  "cfdb>=0.10.0",
-  "ebooklet>=0.10.5",
+  # version that can.
+  # >=0.11 (cfdb and ebooklet): ebooklet 0.11's write-order groups are remote format 3, which
+  # 0.10 clients refuse ("upgrade ebooklet"), and publish() forwards group_bytes, which only
+  # cfdb 0.11's open_edataset accepts. The commons catalogue itself is a format-3 remote.
+  "cfdb>=0.11.0",
+  "ebooklet>=0.11.0",
   "booklet>=0.12.6",
   "pyproj",
   "shapely>=2.0",
```
