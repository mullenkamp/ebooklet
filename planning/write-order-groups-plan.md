# Plan: write-order groups in ebooklet (remote format 3), replacing hash grouping

## Core

**Purpose:** make appending to a grouped ebooklet remote upload only the new data, not most of the
dataset.

**Decision:** replace hash grouping (`blake2b(key) % num_groups`, `ebooklet/utils.py:202`) with groups the
WRITER assigns at push time.
- Groups follow local-file write order and are filled to a byte target `group_bytes` (default 32 MB,
  writer-side only, never stored on the remote).
- Each key's gid lives in its remote-index entry (15 → 19 bytes). Readers take the gid from the index, so
  ebooklet never learns cfdb's key format.

**Mechanism:**
- New keys, in physical (= write) order, fill the tail group (pulled if not local) and then fresh groups
  numbered `max(manifest)+1`.
- Existing keys keep their gid, and updates repack their group in place.
- Deletes are lazy: they remove the index entry only; a group left with no live members is dropped.

**Scope:**
- Grouped remotes become format 3, and the hash path is deleted outright.
- Per-key remotes stay format 2, byte-identical (the 3 ECan datasets).
- There is **no migration machinery.** The grouped remotes (12 WRF-3k, esa-sst, the catalogue) have no
  outside users (Mike, 2026-10-07), so they are hydrated, deleted and republished.

**Top risks:**
1. Deleting a remote whose local file is incomplete. Every file is hydrated with `load_items()` first.
2. Breaking per-key remotes. This needs explicit storage-kind classification and a per-mode format stamp.
3. The commit path: today a crash after the commit, or a lost lock, leaves the local index older than
   the manifest. Combined with the emptied-group rule, that would GC live groups, so it is fixed here.

---

## Context
- **The assessment** (this session; ebooklet's own hash over real cfdb keys):
  - an append's amplification is about the number of keys per group;
  - WRF-3k: 10–28× per yearly extension (~150 GB to add ~12 GB);
  - the 1980s prepend would re-upload ~100 %;
  - C1 at G≈1000: 22.5× over 45 yearly pushes.
- Mike does the prepend after this lands.
- Backlog item: `envlib/OPEN_WORK.md` "[ebooklet] Group by a dataset dimension…".
- **Mike's decisions (2026-10-06/07):**
  - write-order allocation; fill partial groups (pull the tail); 32 MB;
  - read coalescing out of scope;
  - lazy deletes; gid = `max(manifest)+1`; `group_bytes` not stored on the remote;
  - no tail-pull fallback;
  - delete and republish instead of migrating.
- **Plan review `ebooklet-wog-plan-1`** (Fable 5.1, GLM-5.3, Gemini 3.1 Pro). Every finding was verified;
  records are in the scratchpad (`synthesis-…`, `verdicts-…`) and get copied in step 0.
  - The core mechanism was confirmed by execution (write order, gid reuse, stale-reader healing).
  - It changed:
    - the commit path (F1);
    - the `group_bytes` default (G1);
    - the offline sidecar rule (G2);
    - the format-stamp split (L3);
    - the journal version for per-key files (F8);
    - the acceptance criterion (F7);
    - documentation of lazy-delete semantics (F5);
    - the site list (F9a).
  - Its catalogue and rollout findings (F2, F3, F4) are made moot by delete-and-republish.

## Steps

### 0. Housekeeping (first, after approval)
- In `envlib/OPEN_WORK.md`, add these backlog items:
  - **[ebooklet] Coalesce grouped ranged reads.** `get_remote_group_values` (`utils.py:835-897`) GETs from
    the first wanted member to the last. Mike looked into this before and found it harder than expected;
    there is no record in `ebooklet/planning/`.
  - **[ebooklet] Compaction of dead bytes** left by lazy deletes. fsck reports the dead fraction from this
    plan on.
  - Mark the "group by a dimension" item as in progress.
- Copy this plan, the brief, the verdicts and the synthesis into `~/git/ebooklet/planning/`
  (`write-order-groups-*`).
- Memory: correct `project_commons_rcg.md`. `~/.envlib/commons/catalogue.rcg` is a stale 13 July snapshot,
  and publishes go through `~/.envlib/cache/<hash>.rcg`.

### 1. ebooklet 0.11.0

**API** (`main.py`: `open_ebooklet` 1633-1758, `open_rcg` 1767-1872, `_init_common` 370-680,
`RemoteConnGroup` 1519-1527)
- `group_bytes` defaults to a sentinel meaning "unspecified".
  - Explicit `None` means per-key; an int means grouped with that packing target.
  - Unspecified **inherits**: the remote's mode, then the journal's, then for a brand-new remote grouped
    at `DEFAULT_GROUP_BYTES = 32 * 2**20`.
  - **Default change (confirmed by Mike, 2026-10-07):** new remotes, including new EDatasets, are
    grouped at 32 MB unless `None` is passed. Per-key (one object per key) stays available as
    `group_bytes=None`. ingest-base's `tsortho`/`tsforecast` and ECan pass `None` explicitly, so they
    are unaffected.
- A mode mismatch with an existing remote only matters under `'n'`, where it raises. Otherwise the remote
  wins, with a warning if the kwarg was explicit.
- Validate `1 <= group_bytes <= utils._MAX_GROUP_BYTES`.
- `num_groups`: `None` is accepted silently for one release (cfdb-ingest `forecast_archive.py:81` and
  ifs-download `archive.py:72` pass it). An int raises `ValueError` naming `group_bytes`.
- A public `db.group_bytes` property replaces the private `_num_groups`, which is read by ECan
  `repair_precip.py:204` and cfdb `benchmarks/.../plan.py:8`.
- Delete `key_to_group_id`, `next_prime` and `is_prime_small`.

**Remote format** (`utils.py:51-65`, commit metadata `1606-1614`, `remote.py:193-223`)

| Kind | Stamp written | Index entry | Readable by 0.11 |
|---|---|---|---|
| per-key | `format_version` 2 (unchanged, no group fields) | 15 bytes | yes |
| grouped | `format_version` 3 | 19 bytes: ts7 + gid4 + off4 + len4 | yes |
| legacy | 1, or 2 + `num_groups` | — | refused for r/w/c; the message says: hydrate with `ebooklet<0.11`, delete, republish |

- `SUPPORTED_FORMAT_VERSION` becomes the **reader cap only**. The commit stamp is chosen per mode, 2 or 3
  (`utils.py:1611`, which today stamps the constant unconditionally).
- New `remote_session.storage_kind`: None, `'per_key'`, `'grouped'` or `'legacy'`. It replaces the
  `v1_remote` test at `main.py:434-435`, which would otherwise refuse per-key remotes with a "re-create
  it" message.
- `PAYLOAD_VERSION` stays 2. The index bytes carry their own `value_len`.
- New helpers `encode_group_entry` and `decode_index_entry`. Every site that tests `num_groups` or
  slices index entries switches to them:
  - `main.py:436-437, 558, 579-617, 943-950, 1056-1063, 1231-1233, 1265, 1282-1284`;
  - `utils.py:1296, 1313-1318, 1464, 1504, 1524, 1556, 1586, 1611, 1644, 1663-1665, 1674-1675, 1682`;
  - `remote.py:214, 432, 470, 651`;
  - `fsck.py:106, 125, 136`.
- Reword the "format-1/0.10" refusal texts (`main.py:518-523`, `utils.py:106-117`, `errors.py:61-66`,
  `utils.py:226-230, 1512`).

**Commit-path fix (review F1; reproduced on 0.10.5 as `f1/e4.py`)**
- Stamp the local file with the commit timestamp only after the commit PUT succeeds. Today it happens
  before the PUT (`utils.py:1570`).
- On open or pull, if the cached `remote_state` timestamp differs from the remote's, fetch the full
  index. Never refresh the manifest alone, as `main.py:555-562` does today.
- This also fixes the lost update after a broken lock (E4c): the retry published an index without the
  other writer's keys.
- Belt for the emptied-group rule: a manifest gid is dropped only if the index this push holds is the one
  fetched for the remote's current timestamp.

**Pre-existing discard bug (review F9b; reproduced as `f1/e6.py`; fixed here per Mike)**
- The sequence: `del k` removes `k`'s sidecar entry and journals a delete. `db[k] = v` then cancels the
  delete (`journal.record_write`), and `changes().discard()` drops the write. The forced index re-pull
  only runs when journaled deletes are discarded (`main.py:204-220`), so it is skipped. The sidecar
  stays without `k`, and the next push publishes an index without it.
- Fix: also force the re-pull when any discarded written key is missing from the sidecar.
- Regression test: the E6 sequence in both modes, and a mutant that removes the new condition.

**Journal** (`journal.py`)
- Add `storage` ('per_key' | 'grouped' | None), read-mapped from the legacy `num_groups`/`num_groups_set`.
  An int means legacy hash, which resolves to grouped on an absent remote; that is the republish path.
- `deletes` stays a set.
- Write `JOURNAL_VERSION` 2 **only for grouped files**. Per-key files stay v1, so older venvs sharing
  `~/.envlib/cache` keep working (review F8; `journal.py:92` raises before any offline handling).
  Confirmed by Mike, 2026-10-07.

**Allocation** (a new pure function `utils.plan_groups(...)`, called from `update_remote`)
- Changed keys already in the index use their entry gid, and their group is repacked.
- New keys are taken in `loc_map` (write) order and placed while `size + entry <= group_bytes`.
  Otherwise they open gid `max(manifest ∪ this push's gids)+1`. A fresh group always takes at least one
  key.
- **A repack's member list is the index members of that gid plus this push's allocations.** Local keys
  never join a group by themselves; this replaces `utils.py:1268-1274`.
- The tail is `max(manifest)`.
  - Its projected size is its live members, using updated local lengths.
  - It is pulled through the phase-A pull only when new keys land in it.
  - A failed pull fails the group, and the retry tries again.
- A replacement (`'n'`) has no tail and starts at gid 0. Generation tokens keep object names unique.
- Staging:
  - index entries come only from successful uploads;
  - journal clearing of writes is filtered by allocated or entry gid against `failed_gids`;
  - a failed fresh group leaves no index or manifest entry, and its keys are re-allocated on retry.
- Belt: grouped mode requires a 19-byte sidecar before phase A.
- Known growth: updated keys grow their groups in place. For example, the partly filled last band (217 of
  840 rows) fills up, and cfdb rewrites every time-coordinate chunk on an append. Those groups exceed
  `group_bytes`. This is accepted; the benchmark measures it.

**Lazy deletes** (grouped mode; per-key deletes stay eager)
- A delete removes only the index entry, which already happens locally before the push. No group is
  uploaded for it.
- The commit gate (`utils.py:1556`) also fires for grouped deletes, and the `failed_gids` filter for
  deletes (`1525`) goes.
- Dead bytes drop out on the group's next repack. A gid with no live members is dropped through the
  existing emptied-group path (`1436-1439, 1715-1720`).
- A deleted-then-re-set key is simply new.
- **Documented contract changes (F5):**
  - a reader on a stale index can still fetch a deleted key's bytes;
  - deleted bytes stay publicly downloadable until their group is repacked;
  - fsck reports each group's dead fraction.

**Readers and sidecar**
- `load_items`, `_retry_fetch` and `_load_item` use `decode_index_entry`. Grouped mode is detected as
  sidecar `value_len == 19`, re-checked after each index swap.
- A sidecar whose layout mismatches the remote is discarded **only online, immediately before a
  successful fetch** (G2). Offline, it is kept for `keys()` and materialized values.
- `open_remote_index` takes `value_len`. A fetched index whose layout disagrees with `format_version`
  raises.

**fsck and copy_remote**
- fsck accepts per-key 2 and grouped 3, and refuses legacy.
- Unmanifested gids come from index entries.
- New fields: `empty_groups`, `dead_fraction` per group, and `bad_entry_layout`.
- `copy_remote` (`remote.py:470`) branches on `storage_kind`, which covers an empty-manifest grouped
  remote (L6).

**Tests** (223 collected today)
- Mechanical sweep: replace `num_groups=5` / `NUM_GROUPS` with `TEST_GB`.
- Replace the hash-based same-group constructions with write-order constructions, using the
  `gid_of` / `members_by_gid` helpers.
- **Every rewritten test asserts its own precondition.**
- New tests:
  - allocation follows write order;
  - an append touches only the tail, the updated keys' groups and new groups;
  - tail top-up at the exact `<=` boundary;
  - non-local tail pull;
  - an oversized value gets its own group;
  - gid reuse after the tail is emptied, with old readers staying correct;
  - lazy-delete commit with no upload; an emptied group dropped and GC'd; dead bytes dropped on repack;
    delete then re-set;
  - failed fresh group and retry;
  - **crash after the commit PUT and lost lock (F1 cases)**: the reopen fetches the full index, and no
    live group is dropped;
  - stale 15-byte sidecar: discarded online, **kept offline**;
  - legacy refused for r/w/c, with the hydrate/delete/republish message;
  - **republish path**: a fixture local file generated under 0.10.5 (legacy journal `num_groups`, 15-byte
    sidecar) pushed to an absent remote as grouped. Every key, metadata kept, uuid kept;
  - a per-key remote stays stamped `'2'` with 15-byte entries and a v1 journal, plus an opt-in
    cross-version read with 0.10.5;
  - the `group_bytes` sentinel: inherit from a grouped remote, inherit from a per-key remote, explicit
    `None`, a new remote defaults to grouped;
  - fsck fields;
  - `copy_remote` of a grouped remote with an empty manifest;
  - the catalogue (RCG) grouped;
  - the `num_groups` shim.
- **Mutation list** (each must turn a test red; use `PYTHONDONTWRITEBYTECODE=1` and re-mutate after any
  fix):
  - allocation in changelog order;
  - no tail top-up; top-up without the pull;
  - membership from `loc_map`;
  - the commit gate ignores grouped deletes;
  - an emptied group kept;
  - stage entries for failed groups;
  - `<` instead of `<=`;
  - the tail size counts dead bytes;
  - an oversized value goes into the tail;
  - no sidecar layout check; discard offline;
  - legacy classified as per-key;
  - format 3 stamped on per-key;
  - journal v2 on per-key;
  - stamp before the PUT; refresh the manifest without the index;
  - fsck still hashes;
  - an unspecified `group_bytes` treated as `None`.

**Docs:** README "Grouped Storage" and storage format (68-85, 145-168); `docs/ops.md` (45-46, 68-100,
200-201, 225-237, plus "Moving a hash-grouped remote to 0.11: hydrate, delete, republish"); `CLAUDE.md`
57-89; CHANGELOG 0.11.0.

### 2. Benchmark `group_bytes` later (not blocking; Mike's call to run; live uploads go to his B2)
- **DONE 2026-10-07:** `write-order-groups-benchmark.md`. 8–32 MiB are within noise on every read and push; 64 MiB
  and above make full-band reads 1.7–3× slower and push memory larger. 32 MiB stays.
- 32 MB stays the default until this runs (Mike, 2026-10-07). It has not been assessed.
- A desk estimate from cfdb `benchmarks/RESULTS.md`'s B2 request costs (~0.4 s per GET, 15–17 GETs/s on
  10 threads, first-to-last span over-read) suggests 8–16 MB may read better:
  - more parallel GETs for a full band;
  - less over-read for sub-region reads;
  - writes are indifferent at a yearly cadence.
- Benchmark 8, 16 and 32 MB (plus 64 and 128 MB):
  - push MB/s, peak RSS and the upload bytes of a one-band append (a band is ~142 MB);
  - read timings for a point series, a full band field and a 2×2-tile sub-region.

### 3. Downstream code (alongside step 1)
- **cfdb:**
  - `open_edataset(group_bytes=<sentinel>)` passes the sentinel through (`edataset.py:89/122/140`, which
    also fixes the stale "required for flag='n'" text);
  - update `test_edataset.py`, `docs/guide/s3-remote.md`, `cfdb_summary_usage.md` (619-657) and the
    cfdb skill (809-865);
  - floor `ebooklet>=0.11`.
- **envlib:**
  - `publish(group_bytes=...)` (`catalogue.py:992-1008`);
  - update `test_catalogue_live.py`;
  - drop the "prime" advice (`quickstart.md:119`, `publishing.md:9-16`) and update the envlib skill (49,
    79-81);
  - raise the floors.
- **ingest-base:** `tsortho.py:663/682` and `tsforecast.py:459/478` pass `group_bytes=None` explicitly;
  README:397; base-image rebuild. Also ECan `repair_precip.py:204` → `.group_bytes`.
- **wrf-3k:**
  - `config.py`: per-dataset `group_bytes` replaces `num_groups`, and the stale hash comments are dropped;
  - `build.py:177`; `publish.py:113-143` (the None-guard switches to `group_bytes`);
  - `PROVENANCE.md:132-145` and `README.md:146-149`.
- **esa-sst:** `config.py:129-133` and `publish.py:103-111`.

### 4. Hydrate, delete, release, republish (each run is Mike's, supervised)
1. **Freeze publishing** on every machine.
2. **Hydrate and delete, with the current 0.10.5 locks**, on the machine that holds each archive.
   - For each of the 12 WRF-3k datasets and esa-sst:
     - open the local file against its remote and run `load_items()`;
     - assert no failures, and that the local key count equals the remote index count;
     - then `delete_remote()` on its member key.
   - Finally `delete_remote()` the catalogue (`envlib-commons/catalogue`).
3. **Release** ebooklet 0.11.0, then cfdb, then envlib (Mike publishes; verify by PyPI presence).
   Refresh the locks in all seven repos with `uv lock --upgrade-package`.
4. **Republish with 0.11.**
   - Re-run the wrf-3k gates: hydration changes file size and mtime, and the gate is bound to them.
   - Precipitation, soil moisture and altitude may lack band manifests (review sub-reader, unverified).
     Check that first; if they do, use a direct `cat.publish()` or add the manifests.
   - Then `publish.py` per dataset, which uses the same member keys. The first publish recreates the
     catalogue, grouped by default.
   - esa-sst through its `publish.py`.
5. **Re-register the 3 ECan entries** with `cat.register(...)`. They are per-key and unchanged, so no
   data push.
6. **Verify:**
   - fsck on each remote and on the catalogue;
   - `verify_remote.py` (wrf-3k) and `verify.py` (esa-sst);
   - `Catalogue()` lists 16 entries with the same dvids as today (the list was captured 2026-10-07);
   - delete your own `~/.envlib/cache/*.rcg` caches.
7. **MEGA remotes** (`era5_cfdb`, `sst_cfdb`, private, wrf-model-eval): hydrate, delete and republish like
   the others (Mike, 2026-10-07). Hydrate and delete in sub-step 2 with the current lock. Republish with
   0.11 after updating `push_era5_remote.py`, `push_sst_remote.py` and
   `tests/test_transfer_guards.py:36-40, 201-202` (`NUM_GROUPS` → `group_bytes`), and refresh
   wrf-model-eval's lock.

### 5. Docs and comments sweep (end of implementation, per Mike)
- Update every comment and doc that describes hash grouping or `num_groups`, including the assessment's
  side findings:
  - the WRF-3k comments assume an append fills whole bands, but the record ends partway through one;
  - the "append-mostly favours fewer, larger groups" comments (esa-sst `config.py:129-133`,
    wrf-model-eval `push_sst_remote.py:29-34`) are backwards under hash grouping and moot under 0.11.
- Grep every repo in the stack for `num_groups`, "hash", "prime" and "repacks whole". The list is in the
  downstream map in this session.
- Sweep memory and `OPEN_WORK.md` for stale references (CLAUDE.md §5).

## Verification
- **Step 1:** fake-S3 suites with `uv run pytest`; the live suites on Mike's go; the full mutation list;
  the cross-version per-key read with 0.10.5.
- **Step 3:** cfdb with `--ignore=test_edataset.py` locally, then live; envlib metadata and vocabulary
  tests, then the live catalogue tests on Mike's go.
- **Step 4:**
  - `load_items()` succeeds with counts matching before each delete;
  - fsck is clean (0 orphans, 0 missing, 0 empty groups);
  - the verify scripts' read-back through the public URL passes;
  - the catalogue has 16 entries with the same dvids, and the ECan datasets still open.
- **The goal:** a one-band append to a scratch copy of a WRF-3k dataset uploads the new data plus the
  tail, plus the groups of updated keys (the partial band and the coordinate chunks), and nothing else.
  Read this from the `ebooklet.push` byte totals.

## Review
- Plan review `ebooklet-wog-plan-1` is done (above).
- **Code review after implementation:** propose to Mike, launch nothing without his go. Triggers 1, 2 and
  3 all fire (a gate-like allocation and commit path, public single-copy data after the deletes, and one
  author for code and tests).

---

## Implementation notes (2026-10-07)

Everything below is staged, not committed or released.

**Deviations from the plan, and why:**
- **The `num_groups` shim keeps the old meaning of an explicit `None` (per-key).** The plan said "None
  accepted silently", but `cfdb_ingest/forecast_archive.py:81` and `ifs_dl/archive.py:72` pass
  `num_groups=None` meaning per-key. Swallowing it would have made new forecast archives grouped. Both
  factories' `num_groups` now defaults to the `UNSET` sentinel: explicit `None` maps to `group_bytes=None`,
  an int raises, and passing both raises. cfdb forwards each argument only when given.
- **Mutation testing removed two redundancies.** The deletes-only commit is owned by the commit gate alone
  (the extra `updated = True` was dropped); the offline clause of the sidecar-discard rule was implied by the
  reader condition and was dropped.
- **Hydrate and delete (step 4.2) is a standalone script:**
  `planning/write-order-groups-migration/hydrate_and_delete.py`, run under ebooklet 0.10.5 with
  `uv run --no-project`. It needs no repo environment, so it works while the repos already carry the 0.11
  changes. It refuses on pending local changes, `load_items()` failures, missing or older local keys, and
  non-hash-grouped remotes. `--catalogue --backup-key` copies the catalogue server-side before deleting it.
  Exercised against the 0.10.5 fixture and a grouped RCG (scratchpad `hyd/t_hyd*.py`).
- **Two additions to step 4.**
  - ECan's raw image (base 0.5.0) and flow-forecast-app-envlib's lock must move to the new stack BEFORE the
    catalogue is rebuilt: both read the catalogue, and 0.10 refuses format 3.
  - After republishing, update the publication record in wrf-3k's `PROVENANCE.md` (its layout row still
    says 1999 hash groups).

**Proposed versions** (Mike decides at release): ebooklet 0.11.0, cfdb 0.11.0, envlib 0.1.8,
envlib-ingest-base 0.5.0. The floors are raised accordingly in cfdb, envlib, ingest-base, ECan, wrf-3k,
esa-sst and wrf-model-eval.

**Mutation testing:** fake-S3 suites, `PYTHONDONTWRITEBYTECODE=1`, a fresh copy of the package per mutant.
24 mutants, 23 killed. The survivor is `copy_remote` branching on the manifest instead of the storage kind:
no reachable state tells the two apart (an empty grouped manifest implies an empty index), so it is a
clarity change.

**Also fixed:** `test_generational.py`'s GC-failure test was vacuous (it patched `delete_object`, while GC
calls `delete_objects`); it now fails GC for real, and is mutation-checked.

## Code review `ebooklet-wog-code-1` (2026-10-07)

Arms: Fable 5.1, GLM-5.3 and Gemini 3.1 Pro. The records are in `write-order-groups-code-review-*`. All
three arms confirmed the write-order mechanism by execution. What changed, as decided by Mike:

| # | Change | Why |
|---|---|---|
| A1 | `update_remote` reloads the session's db metadata after the commit (`_note_commit` and the post-push reloads in `Change.push` are gone) | F1, same-session route: the session did not know its own commit had created the remote, force-pulled it and reconciled away the failed groups' hydrated values |
| A2 | A push to an absent remote journals every changelog key before uploading | F1, crash route. 0.10.5's `delete_remote()` keeps `remote_state.remote_ts` (the reconciliation watermark), so the route is live in the migration recipe |
| A3 | `hydrate_and_delete.py` checks against a fresh GET of the db object, backs up and deletes inside the locked session, and gives a clean ABORT on a remote 0.10 cannot read | F2: a stale sidecar passed the check and the remote was deleted (case 7 in `hyd/t_hyd.py`; the old comparison deletes, the new one aborts) |
| B1 | `group_bytes` and every argument after it are keyword-only (`open_ebooklet`, `open_rcg`, both classes, cfdb `open_edataset`) | it took `num_groups`' positional slot. None of 494 call sites in `~/git` passes positionally past it |
| B2 | Tests for the tail size at the updated length, the pull-time re-fetch, the pull-time legacy refusal and the adoption's journal write | their mutants survived |
| B2′ | The layout rule's `index_fetch_suppressed` clause was deleted, not tested | suppression needs a legacy remote, which is always `initialized`, so the clause was implied and no test could pin it |
| B3 | `plan_groups` returns only the assignment | its caller discarded the other two values |
| C1 | Removed the dead delete machinery (`committed_delete_keys`, the `key in deletes` filter, the commit-time `set_storage`, `STORAGE_LEGACY_HASH`); the empty-group branch became an assert | deleted keys leave the sidecar at `__delitem__`; a repacked group always holds a changelog key |
| C2 | `StaleIndexError` removed; the check raises an internal `RuntimeError` | unreachable through the API |
| C3, C4 | No change (grouped-only journal v2; the `_NOT_GIVEN` sentinels stay) | |
| D1 | `update_remote` returns `(committed, failures)`, and `PushResult.updated` is `committed` | a push whose every upload failed reported `updated=True` with nothing committed |
| D2 | The open path stamps the local file after it ingests a fetched index (last, as `_pull_remote_index` does) | readers re-downloaded the whole db object on every open after one remote change |
| D3 | A reader with no remote (offline, or the remote is gone) takes its mode from its sidecar's layout; an explicit `group_bytes` for the other mode warns and loses, like "the remote wins". The sidecar-discard rule lost its offline exception, which this makes unnecessary by construction | `db.group_bytes` reported grouped for a per-key cache. Writers never infer: a 0.10 hash-grouped sidecar is 15 bytes too |

**Mutation testing after the fixes:** 38 mutants, 37 killed. Each new fix (A1, A2, B1 ×2, D1, D2, D3, D3's
reader-only restriction and its explicit-argument precedence) and each B2 pin has its own mutant, and each
is killed. The only survivor is still the `copy_remote` clarity change.

**Pre-existing item held for later (E1):** see `envlib/OPEN_WORK.md`.

## Code review `ebooklet-wog-code-2` (2026-10-07)

Arms: Fable 5.1 and Gemini 3.1 Pro, on the changes since round 1. The records are in
`write-order-groups-code-review-2-*`. Applied, as decided by Mike:

| # | Change | Why |
|---|---|---|
| R1 | `update_remote` clears `replace_pending` in the journal write that clears the committed keys; `Change.push`'s later clearing is gone | Fable F1 (verified, both modes): A1's post-commit HEAD sat between the two writes; a failed HEAD left `replace_pending` with no written keys, and the retry purged every local key and committed an empty database |
| R2 | An invalid recorded `group_bytes` is ignored with a warning | both arms: `int()` on a malformed value failed every open |
| R3 | `Catalogue.publish`: `group_bytes` and `verify_objects` keyword-only | both arms: `num_groups`' old positional slot |
| R4 | Dead clauses removed: `and not index_fetch_suppressed`; `fetched_manifest is not None` replaced by `index_fetched` | Fable F4 |
| R5 | A failure-free push always removes its changelog | Gemini G4 (pre-existing, cosmetic) |
| R6 | `reconcile_local_with_index` returns None for a skipped scan; pull and open then leave the stamp old | Gemini G2 (pre-existing on pull) |
| R7 | The uuid rule on every push: `Change.push` refuses a remote made from a different local file, except a replacement | Fable O1 (pre-existing): the pre-push adoption's forced pull skipped the open's uuid check, reconciled against a foreign index, and re-stamped the remote with its own uuid |

**Refuted:** Gemini G1 ("the migration recipe loses every hydrated key"). The real sequence (0.10.5
in-session `delete_remote()`, then the 0.11 open and push) keeps all ten keys, because an open against an
absent remote ingests no index. Fable independently ran the real script and two republish routes, clean.
**Rejected:** G3's "assign the session fields instead of a HEAD" (that is the old `_note_commit`).

**Mutation testing after the fixes:** 53 mutants, 52 killed. The survivor is still the `copy_remote`
clarity change. The R1 mutant restores the exact pre-fix placement (cleared after the reload, in a separate
write) and is killed. The R7 replacement exemption was a survivor until a test was added.

## Decision changed after the benchmark (2026-10-07): the remote records `group_bytes`

The plan said `group_bytes` is never stored on the remote. The benchmark showed what that costs: a writer that
omits it (a later append, a second machine, a fresh local file) silently packs at the 32 MiB default, and nothing
tells it what the dataset was created with. Mike's decision: a grouped commit now writes `group_bytes` into the
db-object metadata, and the target resolves as the explicit argument, then the remote's recorded value, then
`DEFAULT_GROUP_BYTES` (`main._grouped_target`, at open and on every index pull). Readers report it as
`db.group_bytes`. A writer that passes another value overwrites it, with no lineage kept, and existing groups keep
their size. Per-key metadata is unchanged. An offline reader has no remote to ask and reports the default.
