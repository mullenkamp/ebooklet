# ebooklet Phase 1 — independent design review brief (pre-implementation)

I'm the author, reviewing my own implementation plan before writing any code, and I want it
independently stress-tested. You are a fresh-context reviewer performing a **critical design
review** of the plan below. Your job is NOT to summarize or to agree: verify the plan's claims
against the real source, probe for failure modes, and hunt for what the plan did **not**
consider. The numbered questions at the end are starting points, not the boundary — the most
valuable findings are the ones not on any list here.

## Ground rules

- Repos in scope (read them broadly — dependency source, callers, related modules):
  `~/git/ebooklet` (the subject; currently released 0.9.5), `~/git/booklet` (local KV store,
  0.12.6), `~/git/s3func` (S3 HTTP layer + distributed lock, 0.9.2). Consumers for context:
  `~/git/cfdb-repos/cfdb`, `~/git/envlib-repos/envlib`.
- **Do not modify, create, or delete any files in any repo.** Throwaway scripts and
  experiments under `/tmp` are encouraged — booklet is pure-local (exercise it directly), and
  `ebooklet/tests/fake_s3.py` is an in-memory S3 session fake that drives the real ebooklet
  remote/push code with no credentials (see `test_delete_safety.py` for usage; push idiom is
  `eb.changes().push()`).
- Do not open machine-local git-ignored config files (e.g. `tests/s3_config.toml` in any
  repo — routine local test configuration, out of scope for this review). No network calls;
  no live S3.
- Background documents in `~/git/ebooklet`: `architecture-assessment-synthesis.md` (the
  adopted plan-of-record this phase implements — includes the per-seam verdicts and the
  roadmap), `CHANGELOG.md` (what 0.9.3–0.9.5 already fixed), `phase0-design-brief.md` +
  `phase0-review-*.md` (the previous round, for process context only).

## Fixed constraints (rulings — verify the plan honors them; do not propose alternatives)

1. **No old-format compatibility**: no legacy read path, no per-remote migration, no journal
   seeding from timestamp diffs. All existing remotes are under the author's control and will
   be re-created once. Old clients must refuse new-format remotes loudly.
2. **One release**: this phase and a follow-up API phase ship together as ebooklet 0.10.0;
   booklet and s3func get their own small releases first. Within this phase, breaking internal
   rearrangement is free; the public API surface should not gratuitously change (the API pass
   is the next phase).
3. Push is always explicit; nothing auto-pushes on close; `sync()` is local-only.
4. No sidecar files (no companion files next to the local booklet file).
5. Previously assessed and dismissed (do not re-propose): CAS/conditional-write commit
   protocols; content-addressed objects; segmented/delta index; multi-writer merge semantics;
   lock heartbeats.
6. Single writer at a time (S3 bakery lock, s3func); readers are lock-free.
7. Serialization direction: new ebooklet-owned formats use **msgspec**; booklet keeps orjson
   and portalocker; uuid6 stays.

Items marked ⊕ in the plan go **beyond** the adopted plan-of-record (author additions from a
design pass) — give those extra scrutiny; they have had less review than the rest.

---

# THE PLAN UNDER REVIEW

## Context

Phase 1 fixes the three structural seams the architecture assessment found: **Seam 2** —
pending changes are memory-only (cross-session deletes silently lost; clock-skewed edits never
push; reads clobber unpushed writes); **Seam 1** — session lifecycle state is smeared across
`_flag`/`uuid is None` checks (the 0.9.4 'n'→'w' mutation, the num_groups reopen warning);
**Seam 3** — group objects are overwritten in place, so every push opens a window where
readers fetch garbage/416 from repacked objects, and a torn push corrupts reader views. Plus
F7 (>4 GiB pack overflow), F5(a)(c) (force_lock deletes live writers' lock tickets; holders
never re-verify their ticket), an fsck tool, and three 0.9.5 residuals.

All file:line refs are against current source (ebooklet 0.9.5, booklet 0.12.6, s3func 0.9.2).

## Settled design decisions (stress-test these)

1. **Manifest-map, not per-entry generation.** Index entries stay 15 bytes
   (ts7+offset4+length4, `value_len=15`); a gid→generation map rides the db object.
   Rationale: all members of a group share one generation, so per-entry gen (22-byte entries)
   is redundant data whose only new degree of freedom is intra-group drift — a pure bug
   class; index+manifest ride the same atomic PUT so there is no consistency advantage; the
   manifest is needed anyway (copy_remote's object list, fsck's live set); entries-unchanged
   means the local index sidecar and RCG are untouched. Checked: no read, recovery,
   partial-failure, or copy path favors per-entry.
2. **Generation token = `uuid.uuid4().hex[:13]`** (the lock_id idiom, s3func locking.py:202).
   No false ordering semantics; collision-proof; fsck ages orphans by S3 listing
   `upload_timestamp`, never by token. (Rejected: push-timestamp — collides under clock skew;
   per-group counters — reintroduce overwrite reasoning.)
3. **Journal = booklet reserved slot, msgspec-JSON** (ruling: ebooklet's serialization/data-
   model work moves to **msgspec**; booklet keeps orjson + portalocker as-is; uuid6 stays).
   The journal is a `msgspec.Struct` JSON-encoded into the slot (booklet's reserved-slot API
   is bytes-in/bytes-out — booklet never sees msgspec): `v:int=1, written:list[str] (sorted),
   deletes:list[str], num_groups:int|None, replace_pending:bool, meta_pending:bool`.
   **Dependency changes (ebooklet pyproject):** add `msgspec`; declare `portalocker` (already
   directly imported at utils.py:234, today only transitive via booklet); the direct orjson
   import is removed entirely — two of ebooklet's four orjson sites die with the `_metadata`
   removal (utils.py:306, remote.py:256), the survivors (remote.py:145 `to_dict` encode,
   main.py:927 RCG user_meta validation) swap to msgspec. RCG *storage* keeps booklet's
   `'orjson'` value serializer (name baked into file headers; identical JSON bytes — no
   format benefit to switching).
4. ⊕ **Two reserved slots, not one** (registry in booklet): slot 1 = journal; slot 2 =
   remote-state cache (manifest + remote user-metadata `{ts,data}` carry-forward). Needed
   because a fresh session whose remote timestamp matches the local stamp never re-fetches
   the db object (`check_local_remote_sync`, utils.py:167-168) — a memory-only manifest would
   make grouped reads impossible after restart.
5. ⊕ **Changelog = union of journal membership and the existing timestamp diff.** The
   plan-of-record says "changelog from journal"; union is strictly safer: booklet auto-flushes
   its write buffer mid-bulk-load, so data blocks can be durable while the journal (persisted
   at sync boundaries, not per-write) is stale — the ts diff catches that crash window; the
   journal catches skewed edits and is the only source for deletes. Cost: the O(n) scan
   already exists (accepted F8 boundary). Journal-only + sequence counter stays the 1.0-era
   option.
6. ⊕ **`replace_pending` persists in the journal.** Today an unpushed `flag='n'` session's
   intent is silently lost on 'w' reopen and the next push half-merges into the old remote
   (neither old nor new). Persisting completes the declared operation (loud UserWarning at
   reopen: "next push replaces the remote"); it is also required by the v1-upgrade recovery
   path (see Stage 3 upgrade mechanics). Cleared only by a successful replacement push.
7. ⊕ **Replacement pushes become upload-then-commit-then-sweep** (replaces the wipe-first
   `delete_remote()` at utils.py:577-579): PUT new gens → single db-object PUT flips readers
   atomically → sweep everything under `db_key + '/'` not in the new manifest (safe under the
   held write lock). A partial replacement push then commits NOTHING — the old remote stays
   intact (today it leaves a wiped, broken remote). Also makes the v1→v2 upgrade push atomic
   for old readers. `delete_remote()` remains as explicit teardown only.
8. **Per-key mode (num_groups=None) keeps in-place per-object semantics** — each PUT is
   object-atomic; generational naming would double churn for no isolation gain. Documented:
   no cross-key snapshot isolation in per-key mode. The v2 payload applies (empty manifest;
   metadata embedded).
9. **User metadata embeds in the db object** (kills the `_metadata` S3 object and its index
   entry — the split-brain). **Conditionally**: embed local metadata iff local slot ts ≥
   cached remote meta ts (slot 2), else carry the cached remote `{ts,data}` forward.
   (Unconditional embedding would wipe remote metadata on the first push from a fresh local
   file — init_bytes contains no metadata block.)
10. **format_version → 2**; the 0.9.5 reader guard makes old clients refuse loudly. 0.10.0
    refuses format-1 remotes for r/w/c (no legacy read path) but allows `flag='n'` replacement
    (carve-outs in Stage 3).

## Stage 0 — booklet 0.12.7: reserved-slot registry (~0.5–1d)

Generalize the metadata-key mechanism (all sites verified):
- New magic key-bytes/hash pairs beside `utils.py:56-58`; a `reserved_key_bytes` frozenset +
  `reserved_key_hashes` frozenset (metadata + 2 app slots).
- Skip-guards become set-membership at the three variable-length scan sites: `utils.py:554`
  (file iter), `utils.py:730` (mmap iter), `main.py:803` (map scan). Fixed-length paths:
  unsupported, mirror `FixedLengthValue.set_metadata`'s NotImplementedError (0.12.5 pattern)
  — ebooklet's journal lives only in the variable-length local file.
- Prune count-compensation: `utils.py:1088/1174` (+ vestigial fixed variants 2028/2099) —
  `metadata_key_added` bool → reserved-hash counter; reserved blocks preserved by prune.
- API modeled on `set_metadata`/`get_metadata` (main.py:81-133): `set_reserved(slot:int,
  data:bytes, timestamp=None)` / `get_reserved(slot, include_timestamp=False) -> bytes|None`.
  Bytes in/out (caller serializes); pre-sync + full flush + discarded `n_extra_keys` exactly
  like set_metadata; never bumps `_n_keys`.
- ⊕ **Count-skew bug fix:** deleting the metadata key (or a reserved key) via `__delitem__`
  decrements `_n_keys` that was never incremented (main.py:613-615) — reserved/metadata
  deletes must not decrement. Regression test.
- Known/documented: `clear()` (ftruncate) destroys reserved slots — callers re-persist after;
  reserved-slot overwrites count as mutations for iterator invalidation; slots are
  iteration-hidden, not lookup-hidden (hash-reachable — fine, internal bookkeeping, not a
  security boundary); files with reserved slots read by booklet <0.12.7 leak them into
  iteration (local-only, author-controlled — document only).
- Tests parallel the metadata suite: invisibility in keys/items/values/timestamps/map (file
  AND mmap read-mode), len() exclusion, survives prune, destroyed by clear,
  mutation-during-iteration, fixed-length raise, count-skew regression. Version 0.12.7;
  changelog. Released to PyPI when the stage closes (ebooklet 0.10.0 pins `booklet>=0.12.7` —
  without the skip-guards the journal slot would leak into iteration and get pushed as a
  group member).

## Stage 1 — s3func 0.9.3: lock safety F5(a)(c) (~0.5–1d)

- **F5(a) age-gated `break_other_locks` by default** (locking.py:391-409): the `timestamp`
  gate is already plumbed (`upload_timestamp <= timestamp` → delete) but defaults to `now` =
  deletes everything including live writers' AND the caller's own tickets. Change: default
  `timestamp = now - default_break_age` (module constant, propose **1 hour**; parameter stays
  for explicit override incl. `now` for force-everything) + **exclude the caller's own
  `lock_id`** unconditionally. Changelog documents the behavior change.
- **F5(c) public `verify()`** (~10 lines from existing private primitives): `self._acquired`
  AND a `lock_id`-filtered listing shows both seq objects — the predicate already written
  inline at locking.py:136-138/307-309. Returns bool.
- Tests in the deterministic tier (`test_lock_model.py`): age-gate filtering with injected
  timestamps, own-ticket exclusion, verify() true/false paths; one live test each in
  `test_locks.py`. Version 0.9.3; changelog. Released when the stage closes (ebooklet 0.10.0
  pins `s3func>=0.9.3`).
- ebooklet side (lands in Stage 2): `force_lock` keeps calling `break_other_locks()` — now
  age-gated by default; docstrings + the TimeoutError diagnostic (main.py:219-241) mention
  the gate and the explicit-timestamp escape hatch; `Change.push` calls `lock.verify()` at
  push start ⊕ and again immediately before the db-object commit PUT (the point of no return;
  one cheap LIST) — failure raises (plain RuntimeError-family for now; the Phase-2 taxonomy
  pass names it properly pre-release). The 0.9.5 "crashed-writer lock-orphan" residual
  closes: force_lock with the age-gated default IS the sweep.

## Stage 2 — ebooklet: journal + Seam-1 decomposition (~2–3d)

**Journal state.** A small mutable `JournalState` (written:set, deletes:set, num_groups,
replace_pending, meta_pending) wrapping today's `_written_keys` (main.py:341) / `_deletes`
(main.py:331). Persisted to reserved slot 1 at: open (load-or-init), `sync()`, `close()`,
push start (via `Change.update`→sync), push success (cleared sets rewritten),
partial-failure push (retained sets), `Change.discard`, after `clear()`, ⊕ and in the
finalizer (pass JournalState into `utils.ebooklet_finalizer`, utils.py:128-136, so GC'd
sessions persist too). NOT per-write (write amplification; the sync-window residual is the
accepted union-changelog gap). `set_metadata` sets `meta_pending`; `set_timestamp` journals
like `set` (it changes push behavior).

**Changelog union** (`utils.create_changelog`, utils.py:478-529): journal.written ∪
timestamp-diff; journal keys missing locally and not deletes → dropped with a **loud
warning** (not a quiet log — it can indicate prune-evicted pending data). Deletes come only
from the journal. ⊕ On push, `journal.written` clears **per successful group** (exact dual of
`retained_deletes`, utils.py:727-736; likewise per successful per-key upload) — otherwise any
partial failure re-pushes every successful group forever.

**Read-your-writes gate:** keys in journal.written are never pulled over — gate at the three
read call sites (main.py:557/575 load_items, main.py:719 _load_item) and the push pull-loop
(utils.py:630-632), via a helper (thread the set into `update_remote`;
`check_local_vs_remote` itself stays pure). ⊕ **Skew normalization:** when the gate protects
a journaled key whose local ts ≤ the remote entry's, `set_timestamp(key, max(now,
remote_ts+1))` before packing + warn — otherwise the edit pushes with the older ts and
readers that materialized the newer remote value never pick it up. This closes the 0.8.4
clock-skew case fully.

**Delete semantics — ⊕ replay-on-swap** (instead of gating six read paths): `__delitem__`
keeps its local-index mutation (main.py:753-762); after every index-handle swap — initial
open and `_pull_remote_index` (inside its `_index_lock` block, main.py:636-644) — replay
`journal.deletes` against the fresh index. Makes `__contains__`/`keys()`/`len`/point
reads/load_items all resurrection-proof with two call sites. Requires the remote-index
sidecar writable in 'r' sessions (`open_remote_index` utils.py:256-259 — safe: the local file
already opens 'w' exclusively in all modes).

**Seam-1 decomposition:** `_flag` frozen after open; `_replace_pending` (journal-backed)
replaces the 0.9.4 'n'→'w' mutation (main.py:174-175), the wipe trigger (utils.py:577-579),
and the 'n' purge gate (main.py:146-155); `_remote_initialized` (maintained by
`_load_db_metadata`) replaces the post-open `uuid is None` checks (utils.py:145/229/764,
main.py:182/291). num_groups rides the journal; resolution = **remote metadata > journal >
kwarg** (kwarg-vs-journal mismatch → ValueError; remote-vs-journal mismatch → warn, remote
wins, journal updated — also when `_pull_remote_index` adopts remote num_groups); kills the
0.9.1 reopen warning (main.py:291-299) for journaled files. Reopen with
`replace_pending=True` warns loudly.

**Guards & edges:** `set`/`set_timestamp`/`__delitem__` reject reserved key strings (today
only RCG.add checks, main.py:920-921); `prune()` must not evict journaled-pending keys
(exclude journal.written from timestamp eviction — silent data-loss laundering otherwise);
`Change.discard` removes discarded keys from journal.written + persists, and discarding a
journaled delete forces an index re-pull to restore the entry (the ts-gated pull won't
re-fetch); 'r' sessions load and honor the journal (read-your-writes is a file property) but
never clear it; `changes()`/`view_changelog` surface journal deletes alongside changed keys.

## Stage 3 — ebooklet: generational immutable groups, format_version 2 (~2–3d)

**Group objects:** `f'{db_key}/{gid}.{gen13}'`, immutable — never overwritten under a live
reference. Old `str(gid)` naming and the in-place overwrite die.

**db-object payload** (body today = raw index booklet bytes; new layout, sections before the
index so metadata/manifest are cheap ranged GETs):
```
magic b'ebooklet-db\x00' (12) | payload_version >H =2 (2) | reserved (2)
| manifest_len >Q (8) | meta_len >Q (8) | index_len >Q (8)
| manifest JSON (msgspec) {"<gid>": "<gen13>"}   (empty dict in per-key mode)
| meta JSON (msgspec) {"timestamp": µs, "data": ...}  (len 0 = absent)
| index: raw FixedLengthValue bytes (value_len=15, unchanged)
```
S3 metadata unchanged except `format_version:'2'` (HEAD-driven paths — `_load_db_metadata`
remote.py:181-211, `get_timestamp`, `check_local_remote_sync` — stay metadata-only;
`init_bytes`/`num_groups` needed pre-body). v1 bodies start with booklet's fixed-file uuid →
unambiguous discrimination. Both S3 sessions and the credential-less http `db_url` path parse
identically (Range works on both; `fetch_remote_index` must accept 206 as well as 200,
utils.py:236).

**Push protocol** (rewrites utils.py:686-804; phases): **A** — changelog union → affected
groups → pull loop against OLD manifest gens (with the Stage-2 gate) → pack (F7 guard).
**B** — PUT each repacked group to a fresh gen; emptied groups PUT nothing (the pre-commit
`delete_object` at utils.py:338 is removed); PUT failures → failures dict, manifest keeps old
gen, index entries untouched, deletes+written retained — composes with existing bookkeeping;
abandoned new gens = invisible orphans. **C (commit)** — build payload (manifest: successful
gids updated, emptied gids removed; meta per the carry-forward rule; synced index bytes) →
`lock.verify()` → single `put_db_object`. On success: clear journal per-group, persist slot
2, `_load_db_metadata`. **D (GC)** — exact-key delete of the pre-push manifest snapshot's
replaced/emptied gens; failures are log-only orphans (nothing depends on them: index+manifest
already dropped the refs; copy_remote is manifest-driven; delete_remote's bounded prefix
still catches them; fsck sweeps them). Replacement (`replace_pending`) pushes: same A–C, then
sweep all of `db_key + '/'` not in the new manifest (decision 7).

**Reader flow:** index entry → gid (key hash) → manifest[gid] → ranged GET `{gid}.{gen}`.
Manifest updates happen **inside `_pull_remote_index`'s `_index_lock` critical section**
(atomic with the index-handle swap — load_items must never pair a new index with an old
manifest); persisted to slot 2 with the remote ts; slot-2 staleness checked against the local
file stamp at open, lazy ranged-GET refresh if needed. **⊕ `_resolve_missing` gains a retry
step (load-bearing):** today it re-pulls the index and classifies but never re-fetches —
under generational GC, "still claimed" is the *common healable* case (key lives in a new
gen). After the re-pull, re-issue the fetch once with the refreshed manifest; only a second
failure raises `RemoteIntegrityError`. **Same function, same round:** the 0.9.5
whole-remote-absence residual — a re-pull that finds `uuid is None` (remote torn down) treats
all outstanding markers as clean absence, not integrity errors.

**Metadata embedding:** `set_metadata` stays local-slot; push embeds per decision 9; readers
refresh the local slot from the payload inside `_pull_remote_index` (never lazily in point
reads — booklet raises on mutation during iteration) iff remote-newer and not `meta_pending`.
The `_metadata` object, its index entry, and all its special-casing die
(utils.py:502-527/565/591-593/607/623-624/691/704; main.py:372/559-561/696-702/722;
remote.py:249-262 becomes the ranged parse). RCG.add's member `get_user_metadata` snapshot
(main.py:939) uses the new parse.

**F7 >4 GiB guard:** incremental check inside `pack_group` (utils.py:77-94) — abort as soon
as `pos` would exceed 2³²−1 (bounds offsets AND avoids materializing the buffer); raise
`GroupTooLargeError`; `upload_group` catches and **returns** it as the per-group failure
entry (worker contract: return, never raise — a raise would crash `future.result()`); the
"run this again" log message excludes non-retryable failure classes.

**copy_remote → manifest-driven** (remote.py:421-511): parse the source db payload (manifest
+ per-key index via `FixedLengthValue` over BytesIO); copy exactly the referenced objects
(gen tokens are namespace-local — copy verbatim); db object last. Orphans stop replicating.

**Upgrade / no-compat mechanics:** format check moves to `utils.open_remote_conn`
(utils.py:139-150 — it has the flag context; `_load_db_metadata` doesn't): format-1 remote +
flag r/w/c → `UnsupportedFormatError`; flag='n' → allowed, with four carve-outs (the 'n' path
today reads old content): (i) skip the old-index fetch — fresh empty index, old keys read as
absent pre-push (documented semantic narrowing of 'n'); (ii) exclude v1 remote num_groups
from the resolution chain; (iii) skip the num_groups conflict guard (main.py:265-270); (iv)
suppress index-body fetches until the v2 commit. HEAD-only reads (uuid/init_bytes/type) stay
— keeping the old uuid is correct (0.9.5 readers then refuse on the format stamp). **Refusal
exception:** 'w' over a v1 remote is allowed iff the local journal carries
`replace_pending=True` (the crashed-'n'-recovery path — reopening 'n' would truncate the
local replacement image). Upgrade recipe (README + changelog): push pending on 0.9.5 →
upgrade → one `flag='n'` re-push per remote; RCG catalogues re-add members after member
remotes are upgraded.

## Stage 4 — fsck (~0.5–1d)

New `ebooklet/fsck.py`: `ebooklet.fsck(remote_conn, delete_orphans=False,
min_age=timedelta(hours=24), check_objects=False) -> FsckReport`.
- Expected set = manifest gens ∪ (per-key mode: index keys) — parsed from the db object;
  nothing else. Orphans = `db_key + '/'` listing minus expected (aged `_writable_probe_`
  leftovers are sweepable orphans; lock tickets live outside the `'/'` prefix).
- Claimed-but-missing = HEAD over manifest gens (≤ num_groups, cheap); per-key entries only
  under `check_objects=True` (O(n)). Also validate every grouped index key's gid ∈ manifest.
- Report mode is lock-free; `delete_orphans=True` **acquires the write lock** (no in-flight
  push can exist) with the age gate as belt-and-braces — protecting a crashed-commit writer's
  pending retry (`force_push` after a failed commit references uncommitted gens; document:
  retry promptly or discard the local file).

## Tests & verification (abbreviated)

Pre-fix proofs on a 0.9.5 worktree (cross-session delete loss; skew-loss; read-your-writes
clobber; delete resurrection after re-pull; 'n'-close-unpushed half-merge; mid-push reader
window). Hermetic tier via `tests/fake_s3.py` (journal round-trips; union changelog incl.
crash-window; replay-on-swap; replace_pending atomic replacement; generational push
immutability/commit atomicity/partial failures; reader heal-via-retry after GC; metadata
carry-forward; F7; v1-fixture refusal + 'n'-over-v1 carve-outs; fsck; RCG E2E). booklet +
s3func test suites extended per their stages. Live B2 tier + downstream cfdb/envlib suites
on overlays.

---

# Specific review questions (starting points — go beyond them)

1. **booklet reserved slots:** are the three variable-length skip sites the complete set of
   key-enumeration paths? Check especially the unclean-close recovery that rebuilds `_n_keys`
   via iteration, prune's recount, `len()` bookkeeping, and the mmap read-mode path. Is the
   two-slot registry API sound, and is bytes-in/bytes-out the right contract?
2. **Journal persistence points and crash windows:** is persisting in the finalizer safe
   (ordering vs the local file's own close inside `utils.ebooklet_finalizer`)? Any persist
   point on a read path (booklet raises on mutation during active iteration)? What exactly is
   lost in each crash window, and does the union changelog actually cover it?
3. **Per-successful-group clearing of `journal.written`:** verify its interaction with (a)
   the flag-'n' purge at main.py:146-155, which consults the written set at push start — a
   partially-cleared set must not cause the purge to delete locally-held keys on a retry; and
   (b) replacement pushes, where a partial phase-B failure commits NOTHING — nothing should
   clear in that case. State the exact clearing rule that is safe for both normal and
   replacement pushes.
4. **Read-your-writes gate completeness:** are main.py:557/575, main.py:719, and
   utils.py:630-632 ALL the sites that can materialize a remote value over a local one? Check
   `get_remote_value`/`get_remote_group_values`/`recover_group_members` callers,
   `_resolve_missing`'s purge path, `load_items`, `map()` (multiprocessing workers), and
   metadata refresh paths.
5. **Delete replay-on-swap:** are "initial open" and `_pull_remote_index` the only two places
   an index handle is (re)created? Does opening the remote-index sidecar writable in 'r'
   sessions have any concurrency consequence the plan missed (two processes, same local
   file — check booklet's portalocker exclusivity)?
6. **Skew normalization:** `set_timestamp` during the push pull-loop — is that mutation safe
   at that point (booklet 0.12.6 allows set_timestamp during iteration — verify it applies
   here), and does it happen BEFORE `create_changelog` snapshots timestamps into the
   changelog file, so the changelog carries the normalized ts?
7. **Manifest-map vs per-entry generation:** is there ANY read, recovery, partial-failure,
   copy, or fsck scenario where a per-entry generation field (22-byte index entries) is
   safer or necessary? This is the one decision the plan-of-record left open — pressure-test
   the manifest-map choice hard.
8. **Payload layout:** parse robustness (magic discrimination vs v1 bodies; ranged-GET
   header reads over both the S3 session and the http `db_url` reader; 206 vs 200 handling;
   what belongs in S3 metadata vs payload). Is `>Q` sectioning right? Is embedding user
   metadata + its timestamp in the payload sufficient to kill every `_metadata` read/write
   path listed?
9. **Push protocol:** enumerate the crash/partial-failure states of phases A–D and verify
   each leaves readers consistent and the writer retryable: phase-B partial PUT failure;
   crash before C; C PUT failure; crash between C and D; D delete failure. Verify "nothing
   depends on phase-D deletes succeeding". Verify the emptied-group lifecycle (no PUT, drop
   from manifest, old gen GC'd) against how `upload_group`/`update_remote` handle empty
   groups today (utils.py:319-341/338).
10. **`_resolve_missing` retry:** is a one-shot re-fetch with the refreshed manifest correct
    and sufficient (writer commits + GCs again between retry steps)? Does the
    whole-remote-absence refinement (uuid None → clean absence for outstanding markers)
    compose with it?
11. **Metadata carry-forward:** fresh-local-first-push preserves remote metadata; the
    `meta_pending` gate protects unpushed local metadata from being overwritten by a pull;
    RCG.add's member metadata snapshot still works. Any path where metadata is lost or
    resurrects?
12. **flag='n' over a v1 remote:** are the four carve-outs complete (what else does the 'n'
    open path read from the remote — trace utils.py:139-208 and main.py:213-341)? Is the
    "'w' allowed iff `replace_pending=True`" exception sound, or does it hide a footgun?
13. **F7 guard:** right placement (inside pack_group) and failure shape (return-not-raise
    through `upload_group` → failures dict)? Is 2³²−1 the correct bound for BOTH the offset
    and length fields given the index entry layout and `pack_group`'s framing?
14. **fsck:** is the expected-set complete (manifest gens; per-key index keys; what about the
    db object itself, probe keys, in-flight push objects)? Is taking the write lock for
    `delete_orphans` + a 24h age gate sufficient protection for the
    `force_push`-after-failed-commit scenario, or should fsck refuse when the db object's
    uuid/timestamp indicates something odd?
15. **s3func lock changes:** is a 1-hour default break age sensible given legitimately
    long-running write sessions (hours-long bulk pushes)? Own-ticket exclusion — any case
    where the caller SHOULD break its own tickets (crashed-and-reopened same-process
    session)? Is `verify()` (own-ticket listing, both seqs present) meaningful under listing
    eventual consistency — check how the existing self-visibility gate handles this
    (locking.py:302-316)?
16. **msgspec specifics:** Struct schema/decode strictness for the journal and manifest
    (int gids as JSON keys — str-encode and re-int on decode?); forward-compatibility of the
    journal blob (`v` field) given the slot may outlive 0.10.0.
17. **Consumers:** anything in cfdb (`edataset.py` — flag='n' then 'w' patterns,
    push-mid-session, `isinstance(result, dict)` partial-failure checks) or envlib
    (`catalogue.py` publish/refresh flows) that these changes break? push() must keep
    returning the same types this phase.
18. **The three 0.9.5 residuals** the plan claims to close (crashed-writer lock orphans via
    the F5 age-gate; whole-remote-absence; copy_remote orphan replication) — are they
    actually closed by what's specified?

# Deliverable

1. A one-line **verdict**: implementable as specified / implementable with changes (list
   them) / not implementable (why).
2. **Findings**, numbered, each with a severity (CRITICAL = would corrupt data or block the
   design; MAJOR = wrong behavior or a hole that must be fixed in the plan; MINOR =
   improvement/doc), the **evidence** (file:line), and a concrete failure scenario.
3. Answers to the numbered questions above (brief where you simply concur).
4. A short **examined-and-held** list: load-bearing claims you checked that held, so the
   author knows what was verified rather than skipped.

Write the full report to `/tmp/phase1_review.md` and also present it in your reply.
