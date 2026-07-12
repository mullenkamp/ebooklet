# Changelog

Notable changes to ebooklet. The format loosely follows [Keep a Changelog](https://keepachangelog.com/);
ebooklet does not promise SemVer — minor versions may change behavior.
Entries for 0.8.3 and earlier were reconstructed from commit history after the fact.

## 0.10.0 (2026-07-13)

Phase 1 of the architecture-assessment roadmap (design dual-reviewed
pre-implementation: `phase1-design-brief.md` + the two `phase1-review-*.md`
reports). Releases together with the Phase-2 API pass as one release.
Requires booklet >= 0.12.7 (reserved slots) and s3func >= 0.9.3 (lock verify
+ age-gated breaking). New dependencies: msgspec (serialization for all new
ebooklet-owned formats); portalocker now declared (was only transitive).

### Added — the persistent pending-change journal (Seam 2)
- **Pending writes and deletions now survive the session** in a hidden journal
  inside the local booklet file (a reserved slot - no sidecar files). Deletions
  were memory-only: closing without pushing silently lost them (0.9.5 could
  only warn); they are now journaled at `del` time (crash-safe), honored by
  reads in later sessions, and applied by the next push, whenever it happens.
- **Clock-skewed local edits now push.** An edit whose timestamp was at or
  before the remote's never entered the timestamp-diff changelog - it silently
  never uploaded (the 0.8.4 warning-only case). The changelog is now the UNION
  of the timestamp diff and the journal, and a skew-stamped journaled edit has
  its timestamp advanced (with a warning) so readers that already hold the
  newer remote value pick up the edit.
- **Read-your-writes.** A read used to pull a newer remote value OVER an
  unpushed local edit, destroying it. Journaled pending writes are now served
  from the local file unconditionally; no remote-driven path (reads, index
  re-pulls, the missing-object re-check's cleanup, `prune(timestamp=...)`)
  may overwrite or evict a journal-pending value.
- **Deletions cannot resurrect.** Every fresh index copy (open and re-pull)
  is replayed against the journal's pending deletions, so `in`/`keys()`/reads
  stay consistent even after another writer's push refreshes the index.
- **`flag='n'` replacement intent survives the session.** Previously an
  unpushed 'n' session's intent was silently forgotten by a 'w' reopen and the
  next push half-merged new keys into the old remote (neither old nor new).
  The intent is journaled: the reopen warns loudly and the next push performs
  the replacement. It clears only when a replacement push fully succeeds.
- **num_groups is remembered.** The journal records the grouping choice
  (tri-state - "per-key chosen" is distinct from "never recorded"), so
  reopening a created-but-unpushed database no longer needs num_groups
  re-passed and the 0.9.1 per-key-fallback warning is gone for 0.10 files.
  A conflicting num_groups kwarg now raises.
- **Journal clearing is commit-scoped**: entries clear only after the db-object
  upload (the push's commit point) succeeds, for exactly the groups that
  commit covered; a partially-failed replacement push clears nothing, so its
  retry cannot purge locally-held keys.
- `changes().pending_deletes` surfaces the journaled deletions;
  `changes().discard()` now also cancels them (restoring their index entries
  from the remote) and updates the journal.

### Changed — storage format 2: generational immutable group objects
The remote storage format is redesigned around one invariant: **no object a
reader can reference is ever overwritten or deleted before the commit that
un-references it**. Both independent architecture reviews converged on this
design unprompted.

- **Group objects are immutable generations**, named `{gid}.{gen13}`. A push
  packs each changed group into a FRESH generation object; the old generation
  is untouched until after the commit. The mid-push reader window is gone: a
  reader's ranged GET can never hit a repacked object at stale offsets.
- **The db object is the atomic commit point.** Its payload now carries the
  manifest (group id -> live generation), the user-metadata section, and the
  index - everything that must change together rides ONE PUT. A torn push
  can no longer corrupt reader views: before the commit, readers see the old
  state fully intact; after it, the new state; never a mixture.
- **The push protocol is upload -> commit -> GC.** New generations upload
  first (invisible); the db-object PUT commits; replaced/emptied generations
  are deleted by exact key afterwards. GC failures are log-only orphans that
  nothing references - `ebooklet.fsck` sweeps them. New index entries are
  STAGED and applied to the local sidecar only after the commit, so a failed
  commit leaves no local reference to uncommitted objects.
- **Replacement pushes (`flag='n'`) no longer wipe first.** They upload, then
  commit (the single PUT atomically flips readers to the replacement), then
  sweep the namespace of everything the new manifest does not reference. A
  partially-failed replacement commits NOTHING - the old remote stays fully
  readable (previously it was left wiped and half-uploaded).
- **The 0.9.3 re-check protocol now HEALS the routine reader race**: a value
  fetch that hits a GC'd generation re-pulls the index+manifest and retries
  once against the new generation. `RemoteIntegrityError` is reserved for
  confirmed faults (the retry also failing). A re-pull that finds the remote
  torn down treats outstanding fetches as clean absence, not integrity faults.
- **User metadata is embedded in the db-object payload** - the separate
  `_metadata` object and its split-brain with the db object are gone.
  Metadata changes commit atomically with everything else; a push from a
  session that never edited metadata carries the remote's section forward
  verbatim; replacement pushes never resurrect the old remote's metadata; a
  skew-stamped local `set_metadata` pushes with its timestamp advanced
  (mirroring the key rule).
- **`copy_remote` is manifest-driven**: it copies exactly the objects the
  source db object references - orphans are no longer replicated.
- **`format_version` is 2**, stamped in the db object's S3 metadata. There is
  NO format-1 read path (a deliberate no-compatibility decision - all
  existing remotes are under the author's control): 0.10 refuses format-1
  remotes for r/w/c with `UnsupportedFormatError`, and pre-0.10 clients
  refuse format-2 remotes via the 0.9.5 reader guard. **Upgrade recipe**:
  push pending changes with 0.9.x, upgrade, then re-push each remote once
  with `flag='n'` (re-pass num_groups - it is not inherited); re-add
  RemoteConnGroup members after the member remotes are upgraded.
- **>4 GiB groups are guarded at pack time** (`GroupTooLargeError`, exported):
  previously an oversized group overflowed the 4-byte offset/length fields
  mid-push. The failure is per-group and marked non-retryable in the log
  (re-shard with a larger num_groups).
- Per-key mode (num_groups=None) keeps in-place per-object semantics: each
  PUT is object-atomic, but there is no cross-key snapshot isolation
  (documented; grouped mode is the recommended layout).

### Added — `ebooklet.fsck`
`fsck(remote_conn, delete_orphans=False, min_age=timedelta(hours=24),
check_objects=False) -> FsckReport`: the manifest makes integrity checking
trivial - it IS the list of objects the database references. Reports orphans
(abandoned/unGC'd generations, aged probe leftovers), referenced-but-missing
objects (real integrity faults), unmanifested group ids, and torn teardowns
(children without a db object). The optional sweep takes the write lock and
age-gates every deletion (default 24h) so an in-flight or promptly-retried
push is never robbed of its fresh uploads.

### Added — lock safety at push boundaries (F5)
- **`push()` re-verifies the write lock** at push start and again immediately
  before the db-object commit. A holder whose lock ticket was broken (another
  client's force_lock) aborts BEFORE writing instead of pushing without mutual
  exclusion; everything stays journaled for a retry.
- `force_lock=True` is now age-gated via s3func 0.9.3: it breaks only lock
  tickets older than 2 hours, so a live writer's session survives an impatient
  force_lock (its own tickets are also never broken). This closes the 0.9.5
  "crashed-writer lock orphans" residual: force_lock is now safe to use
  routinely.

### Changed
- The 0.9.5 close-time "pending deletions will be LOST" warning is removed -
  it is no longer true.
- User keys equal to internal reserved key strings are rejected with
  ValueError (previously only RemoteConnGroup.add checked, and only for the
  metadata key).
- The remote-index sidecar is opened writable in read-only sessions too
  (needed by the deletion replay; safe - the local file holds an exclusive
  OS lock in every mode, so no second process shares the sidecar).
- Internal: session lifecycle state is decomposed (`_flag` is frozen after
  open; the replacement intent lives in the journal; "remote exists" is the
  session's `initialized` property) - the 0.9.4 flag downgrade is gone.
- RemoteConnGroup entry validation and connection serialization use msgspec
  (byte-identical JSON; the stored `'orjson'` value-serializer id in existing
  local files is unchanged and still supported).

## 0.9.6 (2026-07-12)

### Fixed — data loss: deleting a key and re-setting it in the same session lost the key on push
`del eb[key]` followed by `eb[key] = new_value` (or a `set_timestamp`) in the same
session left the key in the pending-deletes set: the push's delete pass runs after
the upload pass and removed the index entry the upload had just written. The push
reported success; fresh readers raised KeyError; the new bytes sat unreferenced in
the group object. Reachable through any delete-then-recreate workflow (e.g. deleting
a cfdb variable and recreating it in one session). `set`/`set_timestamp` now remove
the key from the pending deletes — the mirror of what deletion already did to the
pending writes. Present since deletes were introduced. Found by the Phase-1 design
review (`phase1-review-claude.md`, F-1) and verified against 0.9.5.

## 0.9.5 (2026-07-12)

Phase 0 of the adopted architecture-assessment roadmap (see
`architecture-assessment-synthesis.md`); design dual-reviewed pre-implementation
(`phase0-design-brief.md` + the two `phase0-review-*.md` reports).

### Fixed — data loss: S3 deletes were prefix-based, destroying unrelated objects
- **Emptying one group deleted sibling groups.** `delete_object` issued a string-
  PREFIX delete, and group objects are named by non-zero-padded decimal ids — so
  deleting emptied group `1` also destroyed groups `10`, `11`, `12`, … (with
  `num_groups=127`, up to ~38 sibling groups). The index entries survived, so
  fresh readers then raised `RemoteIntegrityError` on perfectly live keys.
  Reachable through any workflow that empties a group (e.g. cfdb chunk deletion).
  Group deletes are now exact-key, all-versions deletes.
- **`delete_remote()` deleted sibling databases.** It deleted by the bare
  `db_key` prefix, so wiping `mydb` also wiped `mydb2` — and every `flag='n'`
  push calls `delete_remote()` automatically. It now deletes exactly the db
  object plus the `db_key + '/'` children.
- **`delete_remote()` no longer deletes lock objects.** The bare prefix also
  matched `db_key + '.lock.'`, so a `flag='n'` push deleted its OWN live lock
  tickets mid-push, silently dropping mutual exclusion for the rest of the
  session. Trade-off: a teardown no longer sweeps a crashed writer's stale
  tickets — if a later writer blocks on them, use `force_lock=True` (age-gated
  lock breaking is planned).
- **The writability probe wrote outside the database's namespace.** Its test key
  was `db_key + <hex>` (no separator) — a crashed probe left an orphan
  indistinguishable from a sibling database. The probe now writes under
  `db_key + '/_writable_probe_<hex>'`. Pre-existing orphans at the old location
  cannot be swept safely and are left in place.
- **Empty-valued grouped members were silently deleted by a repack.** A grouped
  value of `b''` has index length 0, so the push pull-loop never materialized it
  locally, and a repack from a session that didn't hold it locally dropped the
  key entirely (the lost-keys path). Empty members are now materialized directly
  (their value IS `b''`) before the repack. Pre-existing bug, found by this
  round's design review.

### Added
- **Deleted-member reads are loud.** A member the index claims but its (present)
  group object no longer contains now goes through the 0.9.3 re-check protocol,
  like whole-object 404s already did: one index re-pull, then clean absence (key
  legitimately deleted; stale local value purged) or `RemoteIntegrityError`.
  Previously `key in db` said True while `db[key]` raised a bare KeyError with
  no explanation. Pushes keep the deliberate self-heal (repack the group without
  the dangling member and drop its index entry) — that remains the recovery
  mechanism for the read-side error.
- **`format_version`** stamped into the db object's S3 metadata (currently `1`;
  absence = 1). Opening a remote with a newer stamp raises the new exported
  **`UnsupportedFormatError`** (a `ValueError` — deliberately NOT a
  `RemoteIntegrityError`, since a version mismatch is a compatibility fault, not
  a store-vs-index inconsistency). This is the headroom the planned storage
  redesign will bump.
- **Close-time warning on unpushed deletions.** Deletions are tracked in memory
  only (until the planned change journal); `close()` now emits a `UserWarning`
  when pending deletions would be lost, including after a partial-failure push.
- **Warning on plain-`http` `db_url`** at `S3Connection` construction (readers
  would fetch the database unencrypted). `http` remains usable for local testing;
  silence with `warnings.filterwarnings`. (A plain-`http` `endpoint_url` is a
  separate concern — it carries signed requests — and is not warned about here.)
- **README "Data Formats and Stability" section** documenting the remote
  namespace, db-object metadata, the 15-byte index entry (including that
  `length` may be 0 for empty grouped values and that the fixed `value_len` is
  the layout discriminator), the group pack layout, and the **RCG entry schema
  v1, now declared frozen**.
- **Hermetic test layer** (`ebooklet/tests/fake_s3.py` + three new test files):
  the first credential-free tests for the remote/push paths, driving the real
  session and push code over an in-memory S3 fake. Version/purge semantics still
  belong to the live tier (the fake models one version per key).

### Changed
- `S3SessionWriter.delete_object` now returns the error (or None) instead of
  silently ignoring failures; a failed empty-group delete surfaces in the push's
  partial-failure dict and is retried on the next push (previously it was
  swallowed and the object orphaned).

## 0.9.4 (2026-07-12)

### Fixed — data loss: a second push in a `flag='n'` session wiped the remote again
Every push in a `flag='n'` session began with a full remote wipe, then re-uploaded
only the groups touched by the CURRENT changelog. The first push was correct (wipe
+ full upload), but a second push in the same session — e.g. cfdb's documented
"push mid-session, keep working, push at close" pattern — destroyed everything the
first push uploaded except the groups touched since, while reporting success. The
loss was silent until 0.9.3's missing-object detection made it loud (readers now
raised `RemoteIntegrityError`; before 0.9.3 the missing keys just quietly vanished).

A `flag='n'` session now downgrades to `'w'` once the replacement push completes:
the wipe happens exactly once. A partial-failure push does NOT downgrade, so a
retry redoes the full wipe-and-replace (the remote can never end up half old,
half new). Present since the `flag='n'` contract was introduced (0.9.0).

## 0.9.3 (2026-07-09)

### Fixed
- **A 404 for an object the remote index claims is no longer silently treated as
  absence.** Every value fetch is driven by the index, so a missing backing object
  means either a legitimately-deleted key seen through a stale reader index, or a
  real store inconsistency. Previously both were swallowed (`pass` on 404) and
  surfaced later as a confusing KeyError or silently-skipped `map()` keys far from
  the cause (observed in CI during a B2 service incident). Now ebooklet
  **re-checks, then gets loud**: the index is re-pulled once per operation; keys the
  fresh index no longer claims are treated as cleanly absent (with stale local
  values purged, including metadata, so nothing resurrects); keys it still claims
  raise the new **`RemoteIntegrityError`**.

### Added
- `RemoteIntegrityError` (exported): raised for confirmed remote-integrity faults
  (index claims a key, backing object missing after a fresh re-pull). It subclasses
  `urllib3.exceptions.HTTPError`, so existing handlers keep working — but callers
  with offline fallbacks should catch it FIRST and treat it as an error, not as
  connectivity trouble.
- An internal index lock: the index-handle swap performed by `changes().pull()` (and
  now by the automatic re-check) is serialized against point reads and `load_items`,
  so a re-check in one thread can no longer close the index under a concurrent
  reader. Iteration (`keys()`) concurrent with a pull on the same instance remains
  unsynchronized (pre-existing, documented).

### Changed — push over a missing group object now fails instead of self-healing
Previously, if a group object 404'd during a push's pull-phase, the push silently
dropped the affected members (the "lost keys" recovery for pre-0.8.4 partial-push
remnants) and **succeeded**. Now such a push returns a **partial-failure dict**
naming the affected keys and leaves the changelog intact for retry.

**Recovery:** if the reported keys are genuinely gone (e.g. old partial-push
remnants), `del` each reported key and push again; if the object should exist,
restore it (or re-set the keys' values) and push again.

### Changed (other)
- Requires s3func >= 0.9.2 (transient 5xx/429 responses are now retried with
  backoff at the HTTP layer, so brief provider incidents are absorbed before any of
  the above machinery is reached).
- `requirements.txt` pins refreshed to match `pyproject.toml`.

## 0.9.2 (2026-07-09)

Requires s3func >= 0.9.1 and booklet >= 0.12.6.

### Changed
- Requires booklet >= 0.12.6, which fixes the iterator deadlock (`get()` inside a
  `keys()` loop) and adds dict-style mutation detection. ebooklet's public `keys()`/
  `items()`/`values()` inherit the new contract: interleaved reads during iteration
  now work; mutating the ebooklet you are iterating raises RuntimeError instead of
  deadlocking.

### Fixed
- The push db-object path read the local and index booklet files directly without
  holding their thread locks (it moved the shared file position under a concurrent
  reader's feet); those reads now hold the owning booklet's lock.

## 0.9.1 (2026-07-05)

### Added
- Reopening (`'w'`/`'c'`) an existing local file whose remote is not initialized yet,
  without passing `num_groups`, emits a UserWarning: the first push would silently
  fall back to per-key storage. Re-pass the same `num_groups` until the first push;
  after that the remote metadata takes over.

### Changed
- Requires s3func >= 0.9.0 (rewritten lock election, SigV4 special-character key
  fixes) and booklet >= 0.12.4 — closing a dependency-resolution loop where stale
  environments reproduced already-fixed upload failures.

## 0.9.0 (2026-07-02)

### Added
- `RemoteConnGroup.add(conn, key=..., user_meta=...)`: explicit keys and user
  metadata for RCG members (entry schema v1). Entries are stamped at write time, so
  a metadata-only upsert enters the changelog and pushes. Key charset is validated;
  a stale default-keyed entry for the same member is cleaned up when migrating to
  explicit keys.

### Changed
- `flag='n'` contract: requires `num_groups` loudly, and the push after a wipe
  uploads only what was written in the new session — pre-push reads stay transparent
  but never leak old values into the new database (dangling index entries
  eliminated).
- `prune()` contract pinned: it is local cache eviction only; it never touches the
  remote.

### Fixed
- `Change.pull()` left the instance in a stale state; it now reloads metadata fully,
  re-resolves `num_groups`, and cycles the index file handle fetch-first (with
  finalizer re-registration).
- Early-raise paths during opening (e.g. local/remote UUID mismatch) release the
  remote lock immediately instead of at garbage collection.

## 0.8.4 (2026-07-02)

### Fixed
- Push-integrity round: remote metadata is refreshed after the remote lock is
  acquired, so a push serialized behind a concurrent writer no longer proceeds from
  stale state; and a loud warning fires when a push-overwrite discards a local edit
  whose timestamp is older than the remote's (clock skew).

## 0.8.3 (2026-04-09)

### Changed
- `num_groups` is rounded up to the nearest prime for better hash distribution.

## 0.8.2 (2026-04-07)

### Fixed
- `delete_remote()` fixes.

## 0.8.1 (2026-04-06)

### Fixed
- Bug fixes; more tests.

## 0.8.0 (2026-03-19)

### Changed
- Factory functions renamed: `open_ebooklet()` (EVariableLengthValue) and
  `open_rcg()` (RemoteConnGroup).

## 0.7.0 (2026-03-02)

### Changed
- Reworked S3 group reads.

## 0.6.0–0.6.3 (2026-02-20 – 2026-03-02)

### Added
- Grouped S3 object storage (`num_groups`): keys hash into N groups, each stored as
  one S3 object (0.6.0); `map()` for parallel processing (0.6.1, reworked 0.6.2).

## 0.5.11 and earlier

Pre-changelog history (0.4.x–0.5.11, 2024–2026): the remote-linked booklet model
itself — S3Connection, transparent pulls, explicit push with changelogs, load_items,
metadata handling. See `git log` for details.
