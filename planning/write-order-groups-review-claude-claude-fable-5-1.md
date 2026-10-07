<!-- ai-review-harness
round:      ebooklet-wog-plan-1
agent:      claude
requested:  model=claude-fable-5-1 effort=high
resolved:   model=claude-fable-5-1
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-plan-1.md
scopes:     ebooklet cfdb envlib envlib-ingest-wrf-3k
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-06T20:48:11+13:00
-->

<!-- staged cfdb: 1732 KiB of 5196 KiB (3464 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8184 KiB of 228148 KiB (219964 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1448 KiB (692 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-wrf-3k: 3192 KiB of 292760 KiB (289568 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 7 agent turn(s); the report is whichever is substantive -->
Reading the core ebooklet push/open code (utils.py done, main.py in progress); next I'll run the experiments against the fake S3.
Launching two readers for the downstream repos while I run the ebooklet experiments myself.
Format-gate and migration-path experiments done; now the crash-window, stale-reader and delete-path experiments.
Confirmed a pre-existing lost-update path; now testing stale-reader behaviour and the delete/discard path.
Report written to `/scratch/agent_out/B.md`; the same content follows. Repo is `/scratch/work/envlib-ingest-wrf-3k`. The working tree has uncommitted edits (`PROVENANCE.md`, `README.md`, `dev/plants.py`, `scripts/verify_build.py`); quotes are from the working tree.

## 1. Build write order (`scripts/build.py`, `wrf3k.py`)
- **Per time band, one `convert()` per band.** build.py:5 "one time-chunk band at a time (840 h for the hourly datasets, 360 days for the daily ones)". Loop, build.py:182-189:
  ```
  for b0, b1 in bands:
      w0, w1 = max(b0, lo), min(b1 - stp, hi)
      band_paths = wrf3k.window_files(files, entry, w0, w1)
      ...
      res = _ingest(band_paths, entry).convert(target, start_date=w0, end_date=w1, **kw)
  ```
  Bands come from `grid.time_bands(dt(config.ANCHOR), lo, hi + stp, entry.chunk_shape[0], entry.step_minutes)` (build.py:148). It is not per year or per source file: each band reads its daily files (`window_files`, wrf3k.py:105-109).
- **Band order, new file:** first whole on-grid band, then the rest (build.py:157 `bands = [full[0]] + [b for b in bands if b != full[0]]`). The README's production start 1990-07-01 equals `ANCHOR` (config.py:44), so that build is ascending in time.
- **Band order, existing file:** bands before the stored start go nearest-first, i.e. descending time, then later ones ascending (build.py:159-162 `sorted((b for b in bands if b[0] < stored0), reverse=True) + [...]`). A 1980s prepend would be physically written in reverse band order.
- **Spatial chunk order inside a band:** not found; it lives in cfdb-ingest's `WrfIngest.convert`, which is not in this repo. dev/plants.py:244 only says "one band: assembled in memory, written ONCE".
- **Every build ends with `prune()`** on the local file (build.py:234-245; "reclaim local dead space (prune() is local-only)"). Whether prune preserves physical record order cannot be determined from this repo. build.py:222: "A fresh build reclaims only the re-stamped attributes (~3 kB, measured 2026-10-03 on four write paths)"; more than 1 % reclaimed is warned as "chunks were rewritten" (build.py:227-229).
- **Incremental / resumable:** no checkpoint, but any range can be re-run. build.py:6-8: "Creating, appending, prepending, filling a hole and re-running are all the same call with a different range; re-writing labels already stored is an idempotent overwrite."
- **No push during the build.** build.py:175-176: "A plain-path write to a published file is refused by cfdb-ingest (an unfetched chunk would be blanked); the handle fetches what it needs. Nothing is pushed here." build.py:208: `eds.close()  # no push: publish.py pushes the journaled writes`.
- **`cfdb.open_edataset` at build.py:177 is used only when the file is already published** (`remote = exists and wrf3k.is_remote_backed(out)`, build.py:141; `bool(ds._sys_meta.remote)`, wrf3k.py:134). A first build writes to the plain path (`target = out`).
- **Whether production builds were one pass:** not established. In each of the nine manifests all segments carry a single `verified.at` (temperature: 366 segments at `2026-10-03T19:44:11+00:00`), which shows one gate run, not one uninterrupted build. Build logs are git-ignored and were not inspected.

## 2. Extension, chunking, period, sizes, group target
- **No extension recorded; append and 1980s prepend are both planned and supported.**
  - README.md:124-136 "Extending a published dataset", with `publish.py --dataset $D  # pushes only the changed groups`.
  - README.md:152: "**The planned prepend back to 1980:** the "1980s prepend" column above, with `ERA5_YEARS` in `config.py` updated."
  - README.md:88-90 gives the prepend ranges, e.g. `--start 1980-01-01 --end 1990-06-30T23:00`.
  - PROVENANCE.md:180: "Add a row per extension (e.g. the 1980s prepend) with its date and new range." No such row found.
  - config.py:47 `STATIC_TIME = '1980-01-01T00:00'`.
- **The last band is partly filled:** the last manifest segment is `2025-06-22T00:00..2025-07-01T00:00`, 217 rows of 840 (snow: 185 of 360). An append rewrites that band's chunks (build.py:223 "extending past a partly filled last band, legitimately reclaims more").
- **Chunking:** config.py:181-182 `chunk_shape=(840, 24, 24))  # 840 h = 35 days, so band edges are always midnights`. Daily is `(360, 24, 24)` (config.py:284), soil `(360, 1, 24, 24)` (config.py:293), altitude `(1, 534, 315)` (config.py:306). The grid is 534 x 315 (README.md:23-24), giving 322 chunk keys per band (config.py:142; soil 1,288, PROVENANCE.md:136).
- **Period:** `ANCHOR = '1990-07-01T00:00'` (config.py:44), `ERA5_YEARS = '1990-2025'` (config.py:67). Stored hourly record is 1990-07-01T00:00..2025-07-01T00:00, 306,817 labels in 366 bands (manifests). Precipitation ends 2025-06-30T23:00, 306,816 hours (PROVENANCE.md:176).
- **Sizes:** README.md:103-104: "measured on 1990-07..12 and scaled to 35 years: hourly fields 26-64 GB each (about 400 GB for the eight), SMOIS 1.5 GB, SNOW 0.1 GB, terrain under 1 MB." Precipitation: "11.5 GB; 1999 remote groups (all in use)" (PROVENANCE.md:177). Temperature is 51,826,219,007 B (`file_stamp` in manifests/whole_record/temperature.json). Measured sizes for the other datasets: not found.
- **`num_groups` (config.py):**

  | Dataset | num_groups |
  |---|---|
  | precipitation | 1999 |
  | temperature | 8521 |
  | specific_humidity | 10321 |
  | barometric_pressure | 11197 |
  | wind_speed | 9473 |
  | wind_direction | 9619 |
  | radiation_incoming_shortwave | 5717 |
  | radiation_incoming_longwave | 9767 |
  | evapotranspiration | 4523 |
  | snow_water_equivalent | 23 |
  | volumetric_water_content | 251 |
  | altitude | 1 |

- **Stale hash-grouping comments.** The word "scatter" does not appear anywhere in the repo; the hash-grouping text is:
  - config.py:139-143: "# Chunk keys hashed into num_groups remote objects: prime, ~5.7 MB each over 1990-2025 (as the precipitation), from the 1990-07..12 sample's measured size x 69.5 (the same method gives 1973 for the precipitation: 1999 published at 11.5 GB). Fixed at the first push. Groups are repacked whole: one appended band touches at most its 322 chunk keys' groups. Kept at the precipitation's size, below ebooklet's 10-100 MB guidance, by ruling (Mike, 2026-10-03): the 1980s prepend grows every group by ~1.3x (to ~7.4 MB)."
  - config.py:201-203: "# Chunk keys hashed into num_groups remote objects (prime; ~5.7 MB each for 1990-2025, from the sample's 163 MB per 6 months). Groups are repacked whole, so one appended band re-uploads ~15 % of the dataset and a yearly extension ~85 % (accepted, Mike 2026-09-24)."
  - The same claims are in README.md:147-149 ("ebooklet repacks whole hash groups ...") and PROVENANCE.md:132-138. Also config.py:151 "Fixed at first push" and publish.py:114 "fixed at the first push".

## 3. `publish.py` and `verify_remote.py`
- **`publish.py` flow (95-146):**
  1. Abort if the file is missing or `num_groups is None` (113-115); check the derived dvid (116-118).
  2. `check_gate_record` (119; 71-92, 48-68): verify_build PASS, same path, `n_times`/`time_start`/`time_end`/`file_stamp` unchanged, manifest sha equal to the PASS's and to the `HEAD:` git blob, no `pending_full`.
  3. Build connections; `cat.validate(str(args.path))` (129); print the plan including `num_groups`; `--dry-run` stops here (138-140).
  4. The single push plus catalogue call, publish.py:143: `result = cat.publish(str(args.path), member_conn, commons_conn, num_groups=entry.num_groups)`.
  5. No post-push check; line 145 prints "next: ... verify_remote.py".
- **The S3 key is derived, not configurable:** `member_key` is `f'wrf-3km-nz-{self.name.replace("_", "-")}'` (config.py:149-152). A new key needs a code change; `dev/scratch_cycle.py` monkeypatches it.
- **The gate record binds to path and `[size, mtime_ns]`.** README.md:155-156: "Linking the file to its remote changes its size and mtime, so the old gate record no longer matches the file and `publish.py` refuses it until the gate is re-run."
- **`verify_remote.py` checks:**
  1. Bare `Catalogue(cache=tmp)` query: exactly one entry, dvid match, no credentials in the entry, `db_url` as expected (83-97).
  2. Spatial queries on both sides of the antimeridian (102-105).
  3. Credential-free `ref.open(file_path=tmp / 'consumer.cfdb')` through the public URL; time, x and y equal the local file; then sampled whole bands (107-126).
  4. `ebooklet.fsck` report-only on member and catalogue: asserts `db_object_exists`, `not torn_teardown`, `not claimed_but_missing`; orphans are printed, not asserted (129-135).
- **The content check is sampled, not complete.** It covers the newest band, the oldest band and `--n-bands` (default 2) random bands (`band_rows`, 40-51). Each is fetched with one `.load()` and compared chunk by chunk with `np.array_equal(rv[s].data, lv[s].data, equal_nan=True)` (54-66). By default that is 4 x 322 = 1,288 of about 117,852 hourly chunks.
- **It compares decoded arrays against the local file, not raw stored bytes,** although the docstring calls it "byte-for-byte". Docstring 9-11: "That read-back is the only CONTENT check: ebooklet.fsck ... checks that objects exist, not their bytes."

## 4. Re-download, hydration, moved or rebuilt local files
- `load_items`, hydrate, re-download: not found (grep over `*.py` and `*.md`). The only remote-to-local reads are `verify_remote.py`'s temp consumer file, `scratch_cycle.py`'s `read_back`, and the build's remote handle that "fetches what it needs" (build.py:176).
- Every local open uses `allow_partial=True` (wrf3k.py:133, 142, 154; publish.py:78; verify_remote.py:93, 115), but publish.py:126-127 states "the file is fully local". Any statement that a production file was pruned to partial, re-hydrated or rebuilt: not found.
- `prune()` appears only in build.py (local dead-space reclaim).
- README.md:98-102 is the target-guard bullet: "The published precipitation's working copy must be there as `wrf_3km_nz_precipitation.cfdb` (if it lives elsewhere, move it together with its `.remote_index` sidecar): that file is what an extension writes into before it is pushed." Precipitation predates the multi-dataset layout (config.py:205-206 "Legacy override from the single-dataset repo ...", env var `WRF3K_CFDB_PATH`).
- Manifests bind to an absolute path (`/home/UOCNT/mex10/data/wrf/cfdb/wrf_3km_nz_<name>.cfdb`). manifests/README.md: "Editing one is a deliberate act (moving a file, re-verifying old labels)".
- PROVENANCE.md:141: "**All twelve datasets are published** (Mike, from the machine that holds the archive)". The `verify_remote.py` read-back was "reported by Mike for the precipitation, temperature, soil moisture and terrain (the other eight: to confirm from his outputs)" (PROVENANCE.md:145-147).

## 5. Dependencies
- **pyproject.toml:8-12, no upper bounds:** `cfdb-ingest>=0.8.0`, `cfdb-vars>=0.2.8`, `cfdb>=0.10.0`, `envlib>=0.1.7`, `ebooklet>=0.10.5`. The root `requires-dist` in uv.lock:169-175 is identical.
- **Locked versions in uv.lock:** ebooklet 0.10.5 (l.116-117), booklet 0.12.10 (l.20-21), cfdb 0.10.0 (l.43-44), envlib 0.1.7 (l.132-133), s3func 0.9.6 (l.941-942), cfdb-ingest 0.8.0 (l.69-70).
- **Specifiers that locked cfdb and envlib declare on ebooklet: not found.** uv.lock lists dependency names only for registry packages; the only `requires-dist`/`specifier` block is the root project's. An upper bound such as `<0.11` can be neither confirmed nor excluded here; check the PyPI metadata.
- **Locked cfdb 0.10.0 does not list ebooklet as a dependency at all** (l.46-61: booklet, cfdb-models, cfdb-vars, geointerp, lz4, msgspec, numpy, pyproj, rechunkit, shapely, zstandard). Yet `/scratch/work/cfdb/edataset.py:9` does `import ebooklet`. In this lock ebooklet comes from the root project and from envlib (l.138 `{ name = "ebooklet" }`).
- Sibling checkouts `/scratch/work/cfdb` and `/scratch/work/envlib` are bare package directories with no `pyproject.toml` (`__version__` 0.10.0 and 0.1.7). `/scratch/work/ebooklet/pyproject.toml` has `s3func>=0.9.6` and `booklet>=0.12.8` (l.26, 28); its `__version__` is 0.10.5.

## 6. `dev/scratch_cycle.py`
- Docstring 1-6: "Scratch cycle on the REAL envlib bucket (Mike's go, 2026-10-02), data only, NO catalogue entry: temperature: build band 1 -> gate -> push (scratch key) -> build band 2 THROUGH THE REMOTE HANDLE ... -> gate -> push -> credential-free read-back of both bands through the public db_url (verify_remote.compare_band) -> fsck -> delete_remote -> fsck shows nothing. altitude: build -> gate -> push -> read-back -> fsck -> delete_remote."
- **Key:** forced to `scratch-wrf3k-<name>` by monkeypatching `config.Dataset.member_key` in parent and child (32, 54-61); refuses if the key already holds data (116).
- **Push is direct, not through `Catalogue`:** `cfdb.open_edataset(conn, entry.path, flag='w', num_groups=entry.num_groups)` then `eds.push()`, asserting no `failures` (69-74).
- **Read-back:** credential-free `ebooklet.S3Connection(db_url=...)`, with every band compared (`band_rows(..., 50, entry)`, line 87).
- **Teardown:** `eds.delete_remote()`, then asserts `not rep.db_object_exists and rep.expected_objects == 0 and not rep.orphans` (100-107).
- **Ranges:** 1990-07-01..08-04T23:00, then 1990-08-05..09-08T23:00 (121, 126). dev/README.md:18 records "(2026-10-02, passed)".

## 7. `manifest.py` / `manifests/`
- **They record value hashes per time band, not remote keys.**
  - Header: `{version, dataset, path, dvid, chunk_shape, encoding, coords}` (manifest.py:54-63).
  - Segments: each `{w0, w1, rows, sha256, verified{at, seed, mode, gate_commit, whole_record}}`.
  - The hash is "SHA-256 over the packed integers `dv.dtype.encode(dv[labels].data)` of exactly those labels (C order ...)" (manifest.py:7-9), "located by label, never by row index".
- **They cannot serve as a record of which keys the remote should have:** no chunk-key lists, chunk counts, byte sizes, `num_groups` or S3 object names.
- **They could serve as an independent content check:** re-hashing each band from a remote read-back and comparing to the committed manifest would verify every band without the local file (366 hourly segments, 36 for snow).
- **Coverage gap:** manifests exist for 9 datasets only. No `manifests/<name>.json` for precipitation, volumetric_water_content or altitude. `manifests/whole_record/` holds only `precipitation.json` and `temperature.json` (old whole-record PASS records with verdict, `n_times`, time range, `file_stamp`, checks).
- **Consequence:** publish.py:56-57 aborts without a manifest, so those three cannot be re-published through `publish.py` as the tree stands.
The report is written to `/scratch/agent_out/A.md`. It runs to 171 lines, over the ~120 you asked for; the content below is the same, slightly condensed.

**How I checked:** static read of `/scratch/work/{envlib,cfdb}`, plus ebooklet where a call had to be traced.
- **[RUN]** items were also confirmed by a throwaway probe in `/tmp/cfdb_probe` (local booklet only, no S3).
- **[TRACE]** items are code-reading inferences through ebooklet that I did not execute.
- Nothing under `/scratch/work` was modified.

## ENVLIB (`/scratch/work/envlib/catalogue.py`)

### 1. Reader cache, open mode, exception handling
- **Cache path:** `DEFAULT_CACHE_DIR = '~/.envlib/cache'` (:41); file is `self._cache_dir / f'{_conn_cache_key(source)}.rcg'` (:778-780).
  - The key is blake2b-8 hex of the URL string, or of `f'{endpoint_url}|{bucket}|{db_key}|{db_url}'` for a conn/dict (:148-154).
  - Public source: `PUBLIC_RCG_URL = 'https://b2.envlib.xyz/file/envlib/envlib-commons/catalogue'` (:39), env override at :40/:105.
- **Open:** only in `refresh()`, called from `__init__` (:774):
  `with ebooklet.open_rcg(source, path, flag='r', offline='auto') as rcg:` then `{k: v for k, v in rcg.items() if _HEX24_RE.fullmatch(str(k))}` (:795-796).
  - `rcg.items()` runs `load_items()` over all keys (ebooklet/main.py:733-742), so a reader cache is fully materialized.
- **Caught:** only `ebooklet.UUIDMismatchError` (:797) and `ebooklet.RemoteMissingError` (:809). There is no handler for ValueError, HTTPError, UnsupportedFormatError or OfflineError, and no cache deletion or retry anywhere.
  - `UUIDMismatchError` warns `'RCG source identity mismatch ... Delete the stale cache at {path} to adopt the new remote. Treating this source as empty for now.'` and sets `source_entries = {}` (:802-808).
  - `RemoteMissingError` warns `'RCG source not readable yet (...); treating as empty.'` (:809-815).
- **Migration impact:** if delete and re-push at the same S3 key yields a new uuid (not verified), every reader with an existing `.rcg` cache gets an empty catalogue plus a warning until they manually delete the cache file.
- **Stale-cache fallback:** only through ebooklet's `offline='auto'`, and only for transport errors (ebooklet/main.py:1849-1861; tuple at errors.py:126-133). ebooklet's own typed errors are re-raised: `if isinstance(err, Error): raise`.
- **If `open_rcg` raises `UnsupportedFormatError`:** it is raised at ebooklet/remote.py:203-209, is not caught by `refresh()`, and propagates out of `Catalogue(...)` construction.
  - The user sees a raw traceback: `UnsupportedFormatError: This remote database uses storage format_version 3, but this ebooklet version only supports up to 2. Upgrade ebooklet to open it.`
  - There is no fallback to the local cache even if one exists; pinned by `tests/test_catalogue.py:516-527`.
  - The comment at :792-793 says "pre-0.10 this was silently mislabeled 'not readable yet' by a blanket ValueError catch". It does not say whether 0.10 is an envlib or ebooklet version, and the old source is not here, so deployed-client behaviour is not verified.
- **Remote briefly 404 while a local cache exists [TRACE]:** no error is raised and the reader silently gets the stale cached entries.
  - A 404 sets uuid to None, so `initialized` is False (remote.py:215-221, 172-178).
  - `RemoteMissingError` needs `flag == 'r' and not initialized and not local_file_exists` (ebooklet/utils.py:301-302), so it is not raised.
  - The uuid check is skipped (`if remote_uuid and flag != 'n'`, utils.py:316), and the 'r' cache is deliberately kept (main.py:477-480).
  - Keys that do need a fetch go down the `remote_gone` path, which treats them as cleanly absent and deletes the local value (`del self._local_file[k]`, main.py:1166-1192).
  - With no cache: `RemoteMissingError`, so empty plus warning.
- **Member datasets:** `DatasetRef.open` calls `cfdb.open_edataset(conn, file_path, flag='r')` with cache `{cache_dir}/{dataset_version_id}.cfdb` (:717-721, :669). There is no try/except, so errors propagate raw.

### 2. `publish()` (:991-1025) and `_upsert_entry` (:1105-1142)
1. **Dataset open:** `cfdb.open_edataset(member_conn, local_cfdb_path, flag='w', **edataset_kwargs)` (:1014).
   - `publish(..., num_groups=None, ...)` is a public parameter forwarded as `edataset_kwargs['num_groups'] = num_groups` (:992, :1007-1008).
   - `register` forwards `**open_kwargs` (:1036).
   - Live tests call `publish(..., num_groups=11)` (`tests/test_catalogue_live.py:74,108,133,188,240`).
2. **Push:** validate inside the session, `_apply_derived_attrs`, then `_raise_on_push_failure(eds.push(), ...)` (:1017-1020). Any `result.failures` raises RuntimeError (:61-73).
3. **fsck:** yes, when `verify_objects=True` (the default): `ebooklet.fsck(member_conn, check_objects=True)` (:86).
   - It reads `db_object_exists`, `claimed_but_missing` and `unmanifested_group_ids` (:88-93) and raises `PublishIntegrityError`.
   - It runs on the member dataset only, never on the catalogue RCG.
4. **Catalogue open:** `ebooklet.open_rcg(rcg_conn, self._rcg_cache_path(rcg_conn), flag='c')` (:1108), with no `num_groups` passed.
   - It reads exactly one entry: `existing = rcg.get(dataset_version_id)` (:1109), and returns as a no-op if unchanged (:1136-1137).
   - Otherwise it calls `rcg.add(member_conn, key=dataset_version_id, user_meta=user_meta)` (:1140) and `rcg.changes().push()` (:1141), then `self.refresh()`.

- **Entry key:** `result['dataset_version_id']` (:1107), which is `compute_dataset_version_id`: "blake2b-12 hex of all 11 Identity fields" (metadata.py:232-240, :250). That is 24 hex chars, matching `_HEX24_RE` (:54); it is not the member uuid.
- **Materialization of other entries:** not ensured by the write path, which is a single-key upsert.
  - The trailing `refresh()` does a full `rcg.items()`, but on the `self._sources` cache files.
  - That is the same local file as the write session only if the cache key matches (same conn fields or same URL string).
  - Default `Catalogue()` (public URL string) plus publish with a credentialed S3Connection gives two different `.rcg` files; the producer's write-side file holds only the entries it touched.
  - Only `deregister(delete_data=True)` walks every entry in the write session (`list(rcg.keys())` plus `rcg.get(other_key)`, :1075-1078).
  - No rebuild, migrate or re-push-all helper exists in envlib: not found.
- **`deregister`:** `open_rcg(..., flag='w')` (:1065), `del rcg[...]`, push (:1099-1100). Member deletion goes through `member_conn.open('w')` and `session.delete_remote()` (:1097-1098).

### 3. RCG-level metadata
Not found: there is no `set_metadata`/`get_metadata` call in `catalogue.py`. Everything is per-entry `user_meta` (:1145+). `rcg.add` also snapshots the member's metadata slot as `remote_meta` (ebooklet/main.py:1587-1598).

### 4. ebooklet privates in envlib
Not found in package code (grep for `_num_groups`, `_remote_index`, `_journal`, `remote_index`, `ebooklet.utils`, `rcg._`, `eds._`).

Format-coupled spots that do exist:
- the `num_groups` kwarg (item 2);
- `rep.unmanifested_group_ids` (:92);
- a test fixture with failure key `'_group_3'` (`tests/test_catalogue.py:536`);
- tests that fake a cache with plain `booklet.open(path, 'n', key_serializer='str', value_serializer='orjson', n_buckets=101)` (`tests/test_catalogue.py:462-465`).

## CFDB (`/scratch/work/cfdb`)

### 5. `sys_meta` lives in the metadata slot, not an ordinary key
- Create: `self._blt.set_metadata(msgspec.to_builtins(self._sys_meta))` (main.py:752-753).
- Open: `meta = self._blt.get_metadata()` then `msgspec.convert(meta, data_models.SysMeta)` (main.py:756-758).
- Flush: `sync_sys_meta` (utils.py:194-204).
- Attributes are ordinary keys: `attrs_key_str = '_{var_name}.attrs'` (utils.py:62).

### 6. Chunk key (utils.py:60, 761-768)
```python
var_chunk_key_str = '{var_name}!{dims}'
def make_var_chunk_key(var_name, chunk_start):
    dims = '.'.join(map(str, chunk_start))
    var_chunk_key = var_chunk_key_str.format(var_name=var_name, dims=dims)
    return var_chunk_key
```
- `chunk_start` is the chunk-aligned start index per dimension in physical space (logical index plus coordinate origin), floored to a multiple of `chunk_shape`: `(pc.start//cs) * cs` (indexers.py:333-334, 347-348).
- It is not derived from coordinate values.
- **[RUN]** keys look like `v!0.0`, `v!0.4`, `v!4.0`, `t!0`, `t!4`, and go negative after a prepend (`t!-4`, `v!-4.0`).

### 7. Append and prepend
- **An origin mechanism exists:** `CoordinateVariable.origin` (data_models.py:35), created as `origin=0` (utils.py:687). It is applied at access: `slice(key + coord_origins[pos], ...)` (indexers.py:226, 237-245) and `get_coord_origins` (support_classes.py:947-956).
- **Prepend** (support_classes.py:1229-1248): `new_origin = self.origin - data_diff`, then `self._add_updated_data(chunk_start, chunk_stop, new_origin, updated_data)`, then `self._var_meta.origin = new_origin`. Existing data chunk keys neither change nor get rewritten.
- **Append** (:1251-1267): `chunk_start = (self.origin,)`, `chunk_stop = (self.origin + updated_data.size,)`.
- **Both rewrite every chunk of that coordinate, not just the edge:** `_add_updated_data` loops `rechunkit.chunk_range(chunk_start, chunk_stop, ...)` over the whole coordinate and calls `self._blt.set(key, ...)` for each (:1199-1224). Neither touches data-variable chunks.
- **Partial edge data chunks** are rewritten later by the caller's write, through read-modify-write in `DataVariable.set`: `b1 = self._blt.get(blt_key)` then `self._blt.set(blt_key, write_func(new_data))` (:1613-1621).
- **[RUN]** with 10 `t` values at chunk 4 and `v` at chunk (4,4):

  | Step | Keys set |
  |---|---|
  | append 3 to `t` | `t!0, t!4, t!8, t!12` |
  | write the new rows | `v!8.0, v!8.4, v!12.0, v!12.4` |
  | prepend 3 to `t` (origin becomes -3) | `t!-4, t!0, t!4, t!8, t!12` |
  | write the new rows | `v!-4.0, v!-4.4` |

  `v!0.*` and `v!4.*` kept their original timestamps throughout.
- **Re-keying** happens only in `Dataset.copy`, which re-bases to origin 0 when any origin is non-zero (main.py:504-510). `truncate` deletes orphaned keys (support_classes.py:1332-1378).

### 8. Write order
- `DataVariable.set` iterates `slices_to_chunks_keys` (:1613), which uses `rechunkit.chunk_range` and its `itertools.product(*ranges)` (rechunkit 0.6.0 main.py:130-132, read from `/staging/envlib-ingest-wrf-3k/.venv`).
- That is C-order over the chunk grid in the variable's declared `coords` order: first coord outermost, last fastest. Time is outermost only if it is the first coord.
- **[RUN]** coords `('t','x')` gave `v!0.0, v!0.4, v!4.0, v!4.4, v!8.0, v!8.4`.
- Across calls the order is whatever the caller's loop does.
- There are no threads in cfdb write paths. Parallelism is read/compute only: `multiprocessing.Pool` (main.py:408-420; support_classes.py:1778-1788) and `self._blt.map(...)` (:1773).
- Outside cfdb, ebooklet's `load_items` writes pulled values into the local file from a `ThreadPoolExecutor` with `as_completed` (ebooklet/main.py:921, 999), so the local-file order of pulled chunks is non-deterministic.

### 9. Reads: batched `load_items`, then per-key `get`
`Variable.load()` (support_classes.py:970-984):
```python
failures = self._blt.load_items(indexers.slices_to_keys(slices, self.name, self.chunk_shape))
if failures:
    raise Exception(failures)
```
- It is called before the per-key `self._blt.get(blt_key)` loops in `iter_chunks` (:857-860, :1512-1514, :1526), the rechunker (:190), `map` (:1761), `Dataset.copy` (main.py:514) and `Dataset.load` (main.py:163-165).
- Per-key `get` with no preceding `load_items`: `_get_raw_chunk` (:901-906) and the read-modify-write in `DataVariable.set` (:1614).

### 10. `open_edataset` (edataset.py:137-175)
```python
open_blt = ebooklet.open_ebooklet(remote_conn, file_path, flag, num_groups=num_groups, lock_timeout=lock_timeout, force_lock=force_lock, **kwargs)
meta = None if flag == 'n' else open_blt.get_metadata()
if flag == 'n': create = open_blt.writable
else:           create = open_blt.writable and meta is None
```
- **Flag 'w', remote absent, local present:** local metadata is non-None, so `create=False`. It attaches to the local dataset using the stored `meta['dataset_type']` (:150-153).
- `EDataset.__init__` writes metadata only if the remote flag flips: `if self.writable and not self._sys_meta.remote: ... set_metadata(...)` (:24-27).
- A new dataset is created only if metadata is None both locally and remotely, or with flag 'n'.
- **ebooklet side of the same case:** a UserWarning "the first push will use per-key storage" if `num_groups is None and not journal.num_groups_set` (ebooklet/main.py:609-616). The stale sidecar is also unlinked for writers on an uninitialized remote (main.py:488-500).
- **Privates:** none found (grep for `_blt._`, `open_blt._`, `ebooklet.utils`, `_num_groups`, `_remote_index`, `_journal`, `remote_index` in `cfdb/*.py`). `load_items` is duck-typed via `hasattr(self._blt, 'load_items')` (main.py:731).
- `num_groups` is a named `open_edataset` parameter, documented as "Required when creating a new database" (edataset.py:91, :119-121).

### 11. Rewrites on every open/close
- **None are unconditional.** The close path is `dataset_finalizer`, `sync_dataset`, then `attrs_finalizer` and `sync_sys_meta` (utils.py:207-221). Both write only on a difference (`if old_meta != sys_meta`, utils.py:201; `if old_attrs != attrs`, utils.py:233-238).
- `EDataset.changes()` and `push()` call `self.sync()` first (edataset.py:35-36, :53).
- **One-off writes:** metadata at creation (main.py:753), and the remote-flag flip on the first EDataset open of a locally-created dataset (edataset.py:24-27).
- A real `sys_meta` change, such as any append or prepend, rewrites the single metadata slot plus all chunks of that coordinate (item 7).
- **rechunkit** is pure chunk arithmetic and never touches the store (0 occurrences of "booklet" in its sources).
- cfdb's own `format_version = 1` (utils.py:85) is independent of ebooklet's `SUPPORTED_FORMAT_VERSION = 2` (ebooklet/utils.py:51).
# Review: write-order groups (remote format 3) for ebooklet

**Verdict:** the core mechanism holds up, but I would not start coding until four things change. One planned rule can delete live group objects (finding 1), the catalogue migration is delete-first without a remote backup (2), `migration_check` misses state the push actually publishes (3), and the release order creates a public outage (4).

Everything below was run on the unmodified `ebooklet` working copy (0.10.5, booklet 0.12.10) against `tests/fake_s3.py`, unless marked "reasoned". Baseline: `pytest -q -x -n 4`, ignoring `test_ebooklet.py`, `test_delete_safety_live.py` and `utest_ebooklet.py`, gave `196 passed in 257.38s`. Facts about envlib, cfdb and wrf-3k came from two read-only sub-readers; I re-read `envlib/catalogue.py:782-816` myself and relay the rest with their file:line.

## What I checked and found sound

- **Write order (Q1).** `locations()` order is last-write order across overwrite, auto-reindex/relocation, `prune()`, and in-place `set_timestamp` (E1). Allocation order affects locality only. Every changelog key has a `loc_map` entry (`create_changelog` drops journal keys with no local value, `utils.py:996-1004`); the last point is reasoned.
- **Absent-remote push (Q4).** With journal, sidecar and `remote_state` from the old hash-grouped remote, a push to a new key, or to the same key after `delete_remote()`, sent every *local* key plus metadata and kept the full uuid. An old reader cache reopened cleanly (E2, E12).
- **`v1_remote` claim.** With `SUPPORTED_FORMAT_VERSION = 3`, a format-2 per-key remote is refused for r/w by the unchanged test (E3).
- **Gid reuse and stale readers (Q3).** A reader on an old index and manifest healed through `_resolve_missing` when a generation was replaced and GC'd, when its group was emptied, and when the same gid number was re-populated (E5).
- **Old-client gate (Q5).** 0.10.5 refuses a format-3 stamp on open r/w/n, `offline='auto'`, `get_user_metadata`, `RemoteConnGroup.add` and `copy_remote` (E8). One exception is finding 6.
- **Scatter number.** 2,909 groups, 0.341 reproduced (E9). I did not recompute 36.2 % / 12.4× or the 10–28× range.
- **Live catalogue.** HEAD shows `format_version: 2`, `num_groups: 13`, `RemoteConnGroup`, and `cf-cache-status: DYNAMIC`, so no CDN staleness was observed on the db object.
- **Prepend.** cfdb has a coordinate origin; a prepend does not re-key existing data chunks (sub-reader, `support_classes.py:1229-1248`, probed on a local booklet).

## Findings, most serious first

### 1. A sidecar older than the manifest, plus "drop any gid with no live members", deletes live groups
Class: data loss. Confidence: high on the state (executed), medium-high on the consequence (reasoned; the planned code does not exist).

The plan's claim that index and manifest come from one commit is false for a writer's own file in two reachable cases:

- **Crash after the commit PUT (E4).** The local stamp is written *before* the PUT (`utils.py:1570`), so the reopen skips the index fetch, while `main.py:555-562` refreshes only the manifest. Result: `manifest == remote commit 2: True | sidecar == commit 1: True`.
- **Lock lost mid-push (E4c).** W2 force-locks and commits; W1 then reaches its commit, stamps, and raises `LockLostError`. On reopen W1's stamp is newer than the remote, so its sidecar never sees W2's commit. W1's retry published an index without W2's five keys: `fresh reader: w2 keys visible: []`. This is a lost update on HEAD today.

Today hash grouping hides the first case, because the retry repacks the same groups (E13a: healed). Under the plan:

- **Groups the sidecar has not seen have no live members, so they are dropped and GC'd.** In the crash case this is survivable, since the keys are still journaled. In the lock-lost case they are another writer's groups and the objects are gone; today they are only de-indexed.
- **A topped-up tail keeps the old offsets against the new generation.** Reads still return correct values through the header check, but fall back to whole-group downloads from then on, and fsck cannot see it.

The trigger is realistic: `force_lock` breaks tickets older than two hours, and the migration pushes run for hours.

Fixes:
- Never pair a refreshed manifest with a kept sidecar. On `remote_state.remote_ts != remote.timestamp`, fetch the full index.
- Stop writing the local stamp before the commit succeeds.
- Add `main.py:558` to the site list. It tests `remote_session.num_groups is not None`, which is always false in format 3.
- Add a mutant: "refresh manifest without index".

(b) Not checked: real s3func lock timing (FakeLock never contends); the consequence under planned code; whether the stale-offset layout actually shifts in a given tail repack.

### 2. Catalogue migration is delete-first; two safer routes work on HEAD
Class: data loss, public single-copy. Confidence: high.

The plan backs up only the local `catalogue.rcg`, which is the file whose completeness is in doubt.

- **Producer file may be incomplete.** envlib's `_upsert_entry` opens the RCG `flag='c'`, reads one entry and upserts one (`catalogue.py:1108-1141`), so the producer file holds only the entries it touched. E2-C: a key added from another file was silently lost by delete-then-push.
- **The path is not `~/.envlib/commons/catalogue.rcg` by default.** It is `~/.envlib/cache/<blake2b of conn>.rcg` (`:778-780`).
- **The gap is not harmless (E10-i).** A fresh reader gets `RemoteMissingError`, which envlib turns into an empty catalogue with a warning. A cached reader gets `'ABSENT'` for any entry not already materialised.

Alternatives:
- **Remote backup first (E13b).** `copy_remote` to a backup key, delete, copy back: old cache and fresh reader saw identical entries, fsck clean.
- **Journaled replacement, no delete (E10-ii).** Mark every local key written, set `replace_pending` and `meta_pending`, push. A failed push left the old remote untouched; success swapped in one commit with the uuid kept and old generations swept. I recommend this: it removes the gap and can carry the repoint upserts in the same commit, collapsing steps 4.3 and 4.4.

(b) Not checked: 0.11's suppressed path would have to discard the 15-byte sidecar; I drove the replacement through private journal methods; B2 versioning and purge behaviour; whether other publishers to the commons catalogue exist.

### 3. `migration_check` is not sufficient
Class: wrong data published. Confidence: high.

- **Metadata is not compared.** cfdb keeps `sys_meta` (shapes, origins) in the metadata slot (`cfdb/main.py:752-758`). On a new key the push embeds the *local* slot. E11: the old remote had `{'schema': 2}`, the new key got `{'schema': 1}`, with every key present and none older.
- **Pending journal state is published.** E11: an unpushed draft, an unpushed overwrite and a pending delete all went out, so new ≠ old while the check as specified is clean.
- **The verification is circular.** It compares the new remote against the local file it was pushed from. The old remote stays alive on the new-key path, so compare old index against new index (key set and timestamps) and the metadata section before repointing. Nine datasets also have per-band sha256 manifests in the ingest repo that could be re-hashed from a read-back.

(b) Not checked: Mike's actual files. If one machine wrote everything and nothing is pending, these do not bite. The manifests claim is the sub-reader's.

### 4. The release order creates a public outage
Class: availability. Confidence: high.

- envlib 0.1.7 on PyPI requires `ebooklet>=0.10.5` with no cap; cfdb's extra is `ebooklet>=0.10.0`.
- So once ebooklet 0.11.0 is published, every fresh install of today's envlib resolves it, and 0.11 refuses the legacy catalogue for `'r'`.
- envlib does not catch `UnsupportedFormatError` (`catalogue.py:797-815` catches only uuid-mismatch and remote-missing), so `Catalogue()` raises a raw traceback.
- The plan releases (steps 1–3) before it migrates (step 4), so this lasts days. The same holds for all 13 legacy datasets.

Options:
- **Keep a read-only legacy path in 0.11 for one release** (hash gid, 15-byte entries; writes refused). This also removes the 0.10.5 round-trips for hydration and deletion. I recommend this.
- **Run all of step 4 on a `0.11.0rc1`** and publish the finals minutes after the catalogue switch.

(b) Not checked: the PyPI metadata of older envlib releases. The comment at `catalogue.py:792-793` says an earlier version swallowed `ValueError` as "not readable yet", so some deployed clients may show an empty catalogue instead of "upgrade ebooklet".

### 5. Lazy deletes change two contracts the plan does not mention
Class: semantics. Confidence: medium (reasoned from E5 case 4 and the code).

- **Readers stop converging.** Today a delete replaces the generation, the next uncached read 404s, and the re-pull reconciles: `get(c) after that -> 'ABSENT'`. With the generation unchanged nothing triggers a re-pull, and a stale-index session can still fetch a deleted key's bytes.
- **Deleted values stay publicly retrievable.** Group objects are self-describing and the manifest is public, so anyone can download and unpack them until an unrelated repack. `copy_remote` copies them too.

For a public catalogue, document this and give fsck the dead-fraction report now, with a way to force a repack.

### 6. A 0.10 writer session opened before a format flip pushes format 2 over it
Class: corruption guarded only by the lock. Confidence: high on behaviour.

E8: `pre-opened WRITER: set + push -> OK`, and the remote stamp reverted to `{'format_version': '2', 'num_groups': '5'}`. `Change.push` re-reads remote metadata only when the remote was absent. It matters only for the same-key catalogue step, including a 0.10 publisher that takes the lock during the delete-to-push gap.

(b) Not checked: FakeLock does not model contention, and my experiment flipped the stamp without taking the lock.

### 7. "An append touches the tail plus new groups only" is false for cfdb
Class: wrong acceptance criterion. Confidence: high on the facts, sizes estimated.

- The yearly append in your own script contains 322 existing keys (E9), because the last band is 217 of 840 rows.
- cfdb rewrites *every* chunk of the time coordinate on each append or prepend (sub-reader, `support_classes.py:1199-1224`).
- Those are in-place updates, so their groups are repacked, and the partial-band groups grow about 4× past `group_bytes`.

Cost stays proportional, but the step-4 goal check will fail as written. State it as new data plus a small constant number of groups.

A pulled file's physical order is fetch order (E1e), so hydrating with `load_items()` yields hash-scattered groups and a first append that could touch up to 322 groups.

### 8. `JOURNAL_VERSION` bump for every file
Class: availability. Confidence: medium.

E12: 0.10.5 raises a plain `ValueError` on a v2 journal, even with `offline=True`, and envlib does not catch it. This hits per-key writer files and any cache a 0.11 session dirties. Bump only when the file is grouped; per-key records are otherwise compatible. A 0.10.5 per-key reader never writes the slot (`None` observed), so pure reader caches are safe if 0.11 keeps that behaviour.

### 9. Smaller items
- **Site list is incomplete.** Beyond `main.py:558`, the sites using `num_groups` as the grouped test are:
  - `main.py:436-437`;
  - `utils.py:1296, 1524, 1586, 1644, 1663-1665, 1674-1675, 1682`;
  - `remote.py:214, 432, 651`;
  - `fsck.py:106, 125, 136`.
- **Pre-existing bug the plan makes more authoritative.** `del k; db[k] = v; changes().discard()` leaves the sidecar without `k` and nothing journaled; the next unrelated push deletes `k` remotely, in both modes (E6).
- **Test count.** 223 tests collect (1 deselected), not 213.
- **wrf-3k (sub-reader).**
  - `publish.py` binds its gate to file size and mtime, so every migrated file needs the gate re-run.
  - Three datasets have no manifest.
  - `publish.py:113` aborts on `num_groups is None`.

## Short answers to the remaining questions

- **Q2.** Membership from the index alone is consistent with the committed index, because both derive from the sidecar. The exposure is a sidecar that is wrong: findings 1 and the discard bug. Delete-only pushes detect emptied groups today (E5 case 2).
- **Q6.** A failed fresh group and a failed tail are fine as specified. The untreated case is a commit that landed without local application (finding 1).
- **Q7.** Keep the tail top-up; without it, small frequent pushes such as the catalogue degrade to one group per push. 32 MB is reasonable (reasoned from write order: the 2×2-tile over-read should be bounded by the spatial row stride, roughly 10 MB, rather than the group size), provided the file is band-ordered. The simplest alternative is per-key storage as ECan uses, at about 118k objects per dataset; worth one line in the plan saying why it was not chosen.
- **Q8.** The `num_groups=None` shim covers cfdb's pass-through. Int callers are wrf-3k `build.py:177`, `publish.py:143`, `dev/scratch_cycle.py:71,102`, and the cfdb and envlib tests.

## Evidence

Run as `/opt/envs/ebooklet/bin/python <script>` from `/scratch/exp`. Scripts are given in full for the experiments that refute something; E1, E2-A/B, E3 and E5 follow the same pattern (seed with `open_ebooklet(conn, path, 'n', num_groups=…)`, act through the public API, print) and I can restate them if needed.

**E1 output**
```
a  initial         ['k0', 'k1', 'k2', 'k3', 'k4', 'k5'] n_buckets 7 index_offset 200
a  overwrite k1    ['k0', 'k2', 'k3', 'k4', 'k5', 'k1']
b  after +14 keys  ['k0', 'k2', 'k3', 'k4', 'k5', 'k1', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12', 'k13', 'k14', 'k15', 'k16', 'k17', 'k18', 'k19'] n_buckets 12007 index_offset 1572
c  prune removed 5 order preserved: True index_offset 1484
c  post-prune writes tail: ['k19', 'k3', 'new1']
d  set_timestamp k0 -> position 0 (0 = unmoved; first key in file is k0 )
e  after point reads k15,k2,k9: ['k15', 'k2', 'k9']
e  after load_items()         : ['k15', 'k2', 'k9', 'k16', 'k19', 'k4', 'k13', 'k18', 'k5', 'k1', 'k12', 'k17', 'k8', 'k10', 'k0', 'k6', 'k7', 'k11', 'k3', 'new1', 'k14']
e  producer file order        : ['k0', 'k2', 'k4', 'k5', 'k1', 'k6', 'k7', 'k8', 'k9', 'k10', 'k11', 'k12', 'k13', 'k14', 'k15', 'k16', 'k17', 'k18', 'k19', 'k3', 'new1']
```

**E2 output** (A = new key, B = `delete_remote()` then same key, C = another file added `k_other` and metadata `{'m': 2}` before the delete)
```
old remote : (['k0', 'k1', 'k10', 'k11', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9'], {'m': 1}, '01a11037', 5)
A  open w/ different num_groups -> ValueError num_groups=3 conflicts with this local file's recorded choice of 5. ...
A  journal.num_groups 5 written [] remote_state.manifest {} remote_ts kept True sidecar keys 0
A  push PushResult(updated=True, failures={})
A  new remote: (['k0', ... 'k9'], {'m': 1}, '01a11037', 5)
A  fsck      : FsckReport(db_key='newA', format_version=2, ..., orphans=[], ..., claimed_but_missing=[], unmanifested_group_ids=[])
B  store keys after delete: []
B  push PushResult(updated=True, failures={})
B  re-pushed : (['k0', ... 'k9'], {'m': 1}, '01a11037', 5)
B  old reader cache reopen: keys 12 value ok True meta {'m': 1}
C  push PushResult(updated=True, failures={})
C  re-pushed : (['k0', 'k1', 'k10', 'k11', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7', 'k8', 'k9'], {'m': 2}, '01a11037', 5)
```
C lacks `k_other`. The 8-character uuid prefix is time-based; full-uuid equality is in E12.

**E3 output** (`utils.SUPPORTED_FORMAT_VERSION = 3` set in-process, then open)
```
perkey S3 metadata: {'type': 'EVariableLengthValue', 'format_version': '2'}
grouped S3 metadata: {'type': 'EVariableLengthValue', 'format_version': '2', 'num_groups': '5'}
perkey r -> UnsupportedFormatError This remote database uses storage format_version 2; 0.10 has no format-1 read path. ...
perkey w -> UnsupportedFormatError ...
grouped r -> UnsupportedFormatError ...
grouped w -> UnsupportedFormatError ...
```

**E4 script and output** (crash right after the commit PUT)
```python
import pathlib, tempfile, warnings, io
from unittest import mock
from ebooklet import open_ebooklet, utils
from ebooklet.tests import fake_s3
import booklet
warnings.simplefilter('ignore')
tmp = pathlib.Path(tempfile.mkdtemp(dir='/scratch/exp')); store = {}
conn = lambda k: fake_s3.FakeS3Connection(store, k)
def remote_view(k):
    man, _m, idx = utils.parse_db_payload(store[k][0])
    f = booklet.FixedLengthValue(io.BytesIO(bytes(idx)), 'r')
    out = {key: (utils.bytes_to_int(v[7:11]), utils.bytes_to_int(v[11:15])) for key, v in f.items()}; f.close()
    return man, out
def local_view(eb):
    return dict(eb._remote_state.manifest), {k: (utils.bytes_to_int(v[7:11]), utils.bytes_to_int(v[11:15])) for k, v in eb._remote_index.items()}
with open_ebooklet(conn('d'), tmp/'p.blt', 'n', num_groups=1) as eb:
    eb['old1'] = b'A'*40; eb['old2'] = b'B'*40
    assert eb.changes().push()
man0, idx0 = remote_view('d'); print('commit 1  remote manifest', man0, 'index', idx0)
class Crash(BaseException): pass
real_info = utils.push_logger.info
def info(msg, *a, **k):
    if 'commit succeeded' in str(msg): raise Crash()      # first statement after the successful commit PUT
    return real_info(msg, *a, **k)
eb = open_ebooklet(conn('d'), tmp/'p.blt', 'w')
eb['old1'] = b'a'*90; eb['new1'] = b'N'*40
try:
    with mock.patch.object(utils.push_logger, 'info', info): eb.changes().push()
except Crash: print('-- process "crashed" right after the commit PUT returned 200 --')
eb._finalizer.detach(); eb._local_file.close(); eb._remote_index.close()
man1, idx1 = remote_view('d'); print('commit 2  remote manifest', man1, 'index', idx1)
eb = open_ebooklet(conn('d'), tmp/'p.blt', 'w')
lman, lidx = local_view(eb)
print('REOPEN    local  manifest', lman, 'index', lidx)
print('          manifest == remote commit 2:', lman == man1, '| sidecar == remote commit 2:', lidx == idx1, '| sidecar == commit 1:', lidx == idx0)
print('          journal.written', sorted(eb._journal.written), '| local stamp == remote ts:', eb._local_file._file_timestamp == eb._remote_session.timestamp)
```
```
commit 1  remote manifest {0: '98caea3925154', 1: '1735b84bbfd44'} index {'old1': (21, 40), 'old2': (21, 40)}
-- process "crashed" right after the commit PUT returned 200 --
commit 2  remote manifest {0: 'ff0743f4d51d4', 1: '1735b84bbfd44'} index {'old2': (21, 40), 'old1': (21, 90), 'new1': (128, 40)}
REOPEN    local  manifest {0: 'ff0743f4d51d4', 1: '1735b84bbfd44'} index {'old1': (21, 40), 'old2': (21, 40)}
          manifest == remote commit 2: True | sidecar == remote commit 2: False | sidecar == commit 1: True
          journal.written ['new1', 'old1'] | local stamp == remote ts: True
```

**E4c script (core) and output** (lock lost mid-push; `remote_view` returns sorted manifest gids and index keys)
```python
NG = 7
with open_ebooklet(conn('d'), tmp/'w1.blt', 'n', num_groups=NG) as eb:
    for i in range(6): eb[f'k{i}'] = b'A'*40
    assert eb.changes().push()
with open_ebooklet(conn('d'), tmp/'w2.blt', 'w') as eb: pass
w1 = open_ebooklet(conn('d'), tmp/'w1.blt', 'w'); w1['w1_new'] = b'B'*40
real_upload = utils.upload_group; state = {'done': False}
def upload_and_get_preempted(*a, **k):
    if not state['done']:
        state['done'] = True
        with open_ebooklet(conn('d'), tmp/'w2.blt', 'w', force_lock=True) as w2:
            for i in range(5): w2[f'w2_new{i}'] = b'C'*40
            assert w2.changes().push()
        w1.lock.broken = True
    return real_upload(*a, **k)
try:
    with mock.patch.object(utils, 'upload_group', upload_and_get_preempted): w1.changes().push()
except LockLostError as e: print('W1 push    -> LockLostError:', str(e)[-75:])
w1.close()
w1 = open_ebooklet(conn('d'), tmp/'w1.blt', 'w')
print('W1 reopen  local stamp > remote ts:', w1._local_file._file_timestamp > w1._remote_session.timestamp, '| sees w2_new0:', 'w2_new0' in w1, ...)
print('W1 retry   push:', w1.changes().push()); w1.close()
```
```
start      remote (gids, keys): ([1, 3, 4, 6], ['k0', 'k1', 'k2', 'k3', 'k4', 'k5'])
W1 push    -> LockLostError: anges are retained; re-open the file to re-acquire the lock and push again.
after W2   remote (gids, keys): ([1, 3, 4, 6], ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'w2_new0', 'w2_new1', 'w2_new2', 'w2_new3', 'w2_new4'])
W1 reopen  local stamp > remote ts: True | sees w2_new0: False | manifest gids: [1, 3, 4, 6]
W1 retry   push: PushResult(updated=True, failures={})
final      remote (gids, keys): ([1, 3, 4, 6], ['k0', 'k1', 'k2', 'k3', 'k4', 'k5', 'w1_new'])
fsck      : FsckReport(db_key='d', format_version=2, ..., expected_objects=4, orphans=['4.40ef9a7eb65e4'], ..., claimed_but_missing=[], unmanifested_group_ids=[])
fresh reader: w2 keys visible: []
```

**E5 output** (a, b, c, d all hash to gid 2 with `num_groups=5`; precondition asserted)
```
precondition: gid(a)==gid(b)==gid(c)==gid(d)==2: True | objects ['d/2.5efe841a62654', 'd/4.4b7b9fc3fec44']
case 1: generation replaced + GC while reader holds old index/manifest
  objects ['d/2.d8d1fa76b9654', 'd/4.4b7b9fc3fec44'] | reader manifest gen for gid 2 still exists: False
  reader get(b)  (unchanged member)            -> b'b1'
  reader get(a)  (updated member)              -> b'a2-longer'
case 2: group emptied (dropped from manifest, GC) while reader holds old index/manifest
  delete-only push: PushResult(updated=True, failures={}) | manifest [4] | objects ['d/4.4b7b9fc3fec44']
case 3: same gid number re-populated with a fresh generation (gid reuse), reader still on old view
  manifest {4: '4b7b9fc3fec44', 2: 'a158efbf72d34'} | objects ['d/2.a158efbf72d34', 'd/4.4b7b9fc3fec44']
  reader manifest {2: 'd8d1fa76b9654', 4: '4b7b9fc3fec44'}
  reader get(a)  (deleted; old gid 2 gen)      -> 'ABSENT'
  reader get(c)  (new key in reused gid)       -> b'c1'
  reader get(other)                            -> b'o1'
case 4: reader with a warm copy of a remotely deleted key (today the delete replaces the generation)
  objects before ['d/2.a158efbf72d34', 'd/4.4b7b9fc3fec44'] after ['d/2.b448713bcfa74', 'd/4.4b7b9fc3fec44']
  same-session reader get(c) (cached)          -> b'c1'
  same-session reader get(d) (not cached)      -> b'd1'
  same-session reader get(c) after that        -> 'ABSENT'
```

**E6 script (core) and output**
```python
for ng in (5, None):
    key = f'd{ng}'
    with open_ebooklet(conn(key), tmp/f'{key}.blt', 'n', num_groups=ng) as w:
        w['a'] = b'a1'; w['b'] = b'b1'; assert w.changes().push()
    with open_ebooklet(conn(key), tmp/f'{key}.blt', 'w') as w:
        del w['a']; w['a'] = b'a2'
        ch = w.changes(); ch.discard()
        print(...journal, 'a' in w._remote_index, w.get('a'))
        w['z'] = b'unrelated'; print('   unrelated push:', w.changes().push())
    print('   remote index keys:', remote_keys(key))
```
```
num_groups=5: after del/set/discard: journal written=[] deletes=[] | a in sidecar: False | get(a)=None
   unrelated push: PushResult(updated=True, failures={})
   remote index keys: ['b', 'z']
num_groups=None: after del/set/discard: journal written=[] deletes=[] | a in sidecar: False | get(a)=None
   unrelated push: PushResult(updated=True, failures={})
   remote index keys: ['b', 'z']
```

**E8 output** (0.10.5 against a store entry re-stamped `format_version='3'`, `num_groups` removed; `live_r` and `live_w` opened before the flip)
```
open r (fresh file)                            -> UnsupportedFormatError: This remote database uses storage format_version 3, but this ebooklet
open r (existing cache)                        -> UnsupportedFormatError: ...
open w                                         -> UnsupportedFormatError: ...
open n                                         -> UnsupportedFormatError: ...
open r offline=True (existing cache)           -> OK b'1'
open r offline='auto'                          -> UnsupportedFormatError: ...
session.get_user_metadata()                    -> UnsupportedFormatError: ...
RCG.add(member = format-3 remote)              -> UnsupportedFormatError: ...
copy_remote(format-3 source)                   -> UnsupportedFormatError: ...
pre-opened reader: cached read                 -> OK b'1'
pre-opened reader: uncached read b             -> OK b'2'
pre-opened reader: pull()                      -> UnsupportedFormatError: ...
pre-opened WRITER: set + push                  -> OK PushResult(updated=True, failures={})
remote stamp after that push: {'format_version': '2', 'num_groups': '5'}
```

**E9 output** (your script, with `make_var_chunk_key` exec'd from `cfdb/utils.py`)
```
sample key: temperature!840.24.48
spatial chunks/band 322 | existing keys 117852 | bands 366 | keys in append 3542 (incl. 322 existing partial-band keys)
groups touched 2909 fraction 0.341
```

**E10 script (part ii core) and output**
```python
seed('cat2', 'p2.blt')      # 8 keys, num_groups=5, metadata {'m': 1}, pushed
with open_ebooklet(conn('cat2'), tmp/'p2.blt', 'w') as eb:
    eb.load_items()
    for k in list(eb._local_file.keys()): eb._journal.record_write(k)
    eb._journal.set_replace_pending(True); eb._journal.set_meta_pending(True)
# first push with utils.upload_group patched to return an error for every group, then a normal push
```
```
(i) the gap
  fresh reader open during gap                         -> RemoteMissingError: No file was found in the remote, but the local file was open for read without creating a n
  cached reader open during gap                        -> {'keys': 8, 'k0': b'AAAA...', 'k5': 'ABSENT', 'keys_after': 8}
  live reader session, uncached k5 during gap          -> 'ABSENT'
  live reader session, cached k0 afterwards            -> b'AAAA...'
  cached reader after re-push                          -> {'keys': 8, 'k0': b'AAAA...', 'k5': b'FFFF...', 'keys_after': 8}
(ii) journaled replacement from the producer file (no delete first)
  failed replacement push: PushResult(updated=False, failures={1: 'RuntimeError: boom', 3: 'RuntimeError: b
  old remote untouched after failure: True | db object present: True
  replacement push: PushResult(updated=True, failures={})
  uuid kept: True | old generations swept: True | fsck: [] []
  fresh reader: keys 8 meta {'m': 1} all values equal: True
```

**E11 script (core) and output**
```python
with open_ebooklet(conn('old'), tmp/'p.blt', 'n', num_groups=5) as eb:
    for i in range(4): eb[f'k{i}'] = b'v'*20
    eb.set_metadata({'schema': 1}); assert eb.changes().push()
with open_ebooklet(conn('old'), tmp/'other.blt', 'w') as eb:
    eb.set_metadata({'schema': 2}); assert eb.changes().push()
with open_ebooklet(conn('old'), tmp/'p_pending.blt', 'w') as eb:
    eb.load_items(); eb['draft'] = b'unpublished'; del eb['k3']; eb['k1'] = b'edited-not-pushed'
with open_ebooklet(conn('new1'), tmp/'p.blt', 'w') as eb: assert eb.changes().push()
with open_ebooklet(conn('new2'), tmp/'p_pending.blt', 'w') as eb: assert eb.changes().push()
```
```
old remote              : (['k0', 'k1', 'k2', 'k3'], {'schema': 2})
new key from p.blt       : (['k0', 'k1', 'k2', 'k3'], {'schema': 1})
   journal before push  : written ['draft', 'k1'] deletes ['k3']
   journal after push   : written [] deletes []
new key from p_pending   : (['draft', 'k0', 'k1', 'k2'], {'schema': 2})
```

**E12 output** (journal record re-written with `v=2` into a per-key reader cache via `booklet.set_reserved`)
```
remote stamp: {'format_version': '2'}
0.10.5 reader, own cache: b'1'
journal slot written by a READER session: None
{} -> ValueError This local file carries a journal with version 2, but this ebooklet only supports up to 1. Upgrade ebooklet to | is ValueError: True
{'offline': True} -> ValueError ... | is ValueError: True
{'offline': 'auto'} -> ValueError ... | is ValueError: True
delete_remote + re-push keeps the full uuid: True 01a1103c6e79850fb547b75fb338e2b6
```

**E13 output** (a = retry after the E4 crash; b = `copy_remote` backup, `delete_remote()`, copy back)
```
(a) sidecar stale at reopen: True | retry push: PushResult(updated=True, failures={}) | sidecar+manifest == remote afterwards: True | fsck orphans: ['0.700b688e4aea4']
    fresh reader: {'new1': 40, 'old1': 90, 'old2': 40}
(b) copy_remote -> None
    after delete: cat objects 0 | backup objects 6
    restore copy -> None
    old reader cache after restore: identical entries: True 6
    fresh reader after restore   : identical entries: True
    fsck(cat): [] []
```

**Network reads**
```
$ curl -s https://pypi.org/pypi/envlib/json   -> envlib latest 0.1.7 ['booklet>=0.12.6', 'cfdb>=0.10.0', 'ebooklet>=0.10.5', 'pyproj', 'shapely>=2.0', 'urllib3']
$ curl -s https://pypi.org/pypi/cfdb/json     -> cfdb latest 0.10.0 [..., 'ebooklet>=0.10.0; extra == "ebooklet"', ...]
$ curl -s https://pypi.org/pypi/ebooklet/json -> ebooklet latest 0.10.5
$ curl -sI https://b2.envlib.xyz/file/envlib/envlib-commons/catalogue
HTTP/2 200
content-length: 489245
x-bz-info-format_version: 2
x-bz-info-type: RemoteConnGroup
x-bz-info-uuid: 019f5a675c228596a89af130a2f4521a
x-bz-info-num_groups: 13
x-bz-info-timestamp: 1791273188415646
cf-cache-status: DYNAMIC
```

**Test collection**
```
$ python -m pytest --collect-only -q | tail -1
223/224 tests collected (1 deselected) in 0.20s
```

<!-- finished: 2026-10-06T21:10:36+13:00 exit=0 -->
