# Changelog

Notable changes to ebooklet. The format loosely follows [Keep a Changelog](https://keepachangelog.com/);
ebooklet does not promise SemVer — minor versions may change behavior.
Entries for 0.8.3 and earlier were reconstructed from commit history after the fact.

## 0.10.2 (unreleased)

The deletion-propagation round (design dual-reviewed pre-implementation:
`planning/delete-reconcile-review-brief-gemini.md` + the two
`planning/delete-reconcile-review-*.md` reports). Found live on the envlib commons'
first dataset retraction: a warm local cache kept serving the retracted catalogue
entry indefinitely.

### Fixed

- **Remote key deletions now propagate to warm local caches.** Previously a
  locally-materialized key deleted remotely was served FOREVER: `keys()`/`in`/`len()`
  and point reads all treat local presence as authoritative, a deleted index entry is
  indistinguishable from "nothing newer to fetch", and the only cleanup path (the
  fetch-404 re-check protocol) can never fire for a key whose value is already local.
  Worse, a warm WRITER would silently RE-PUSH remotely-deleted keys on its next push
  (`create_changelog` treats every index-absent local key as a new write). Now every
  fresh index ingest (open-path fetch and `changes().pull()`) reconciles the local
  file against the fresh index: keys it no longer claims are deleted locally and
  logged (`logger.info`; escalates to `warning` when >25% of the cache goes). Guards,
  each load-bearing: reserved/metadata keys never touched; journal-pending writes
  never touched; local values stamped NEWER than the previously ingested index kept
  (they may be unjournaled crash-window writes - the push changelog's timestamp-diff
  rescue still owns those).
- **Re-materialization race closed:** a fetch worker completing against an index
  entry captured before a concurrent pull could re-insert a just-reconciled key;
  fetched keys are now membership-re-checked against the current index before the
  operation returns.

### Notes

- **Upgrade transition:** a PRE-0.10.2 cache that already ingested a post-deletion
  index reads as in-sync and reconciles only at the NEXT actual remote change (the
  freshness gate skips unchanged remotes - an explicit `pull()` against an unchanged
  remote does not heal either). Writers upgrading with warm caches that may hold
  remotely-deleted keys should refresh the cache (or delete the local file and
  re-open) before their next push.
- **Timestamp assumption (documented limitation):** reconciliation discriminates by
  value timestamps and assumes write-time stamps (the default). Explicit
  `set(..., timestamp=...)` stamps defeat it in both directions (a backdated unpushed
  write can be deleted; a future-stamped deleted key survives locally) and are
  incompatible with reconciliation.

## 0.10.1 (2026-07-15)

The push-pipelining round (design dual-reviewed pre-implementation:
`planning/push-pipeline-design-brief.md` + the two `planning/push-pipeline-review-*.md`
reports). Motivated by the first production-scale push (20 GB, 149 groups), which
uploaded one group per ~105 s despite 10 upload threads: the pack step's random
per-key reads under booklet's lock thrashed the disk to ~9 reads/s while the
uplink idled. Requires booklet >= 0.12.8 (`locations()` + `compaction_count`)
and s3func >= 0.9.4 (the read-timeout fix — without it, the concurrent PUTs
this release enables are spuriously killed by s3func's total-request deadline
on slow uplinks, and large-body transfers retry from byte 0 into their own
congestion).

### Changed — pipelined push packing
- **Pack workers no longer read member values through booklet's locked point
  reads.** The changelog sweep (which already walks every block header) now
  also captures each live key's physical `(offset, length)` via booklet's new
  header-only `locations()` iterator — zero extra IO — and each pack worker
  reads its group's values through a PRIVATE file handle in ascending-offset
  order (one forward disk sweep per group, no booklet lock in the hot path).
  Members materialized after the capture (phase-A pulls, empty-value members)
  read through the locked path instead — it is page-cache-hot and guarantees a
  pulled newer value always beats a stale captured offset.
- **`push_packers` kwarg on `open_ebooklet`/`open_rcg` (default 1)**: how many
  pack workers may read the local disk at once (the read gate). Upload
  concurrency is unchanged (`S3Connection(threads=...)`, default 10) and packing
  overlaps uploading regardless, so the default costs nothing while giving
  spinning disks the optimal single-sweep pattern; raise it for SSD/RAID.
  Flows through cfdb's `open_edataset(**kwargs)` and envlib's `publish(**open_kwargs)`
  unchanged.
- **Staged index timestamps now equal the packed timestamps by construction**
  (the entry's ts is taken from the capture/locked read that produced the
  packed bytes, not re-read after the upload). Closes a benign-but-ugly race:
  a concurrent overwrite between pack and bookkeeping could stage a ts newer
  than the packed bytes, leaving the remote internally inconsistent until the
  next push repaired it.
- On Windows, the write-mode booklet's mandatory byte-range lock can deny the
  private-handle reads — those groups fall back to the locked path
  automatically (one warning per push; slower, still correct).

### Added — push progress records
- **The `ebooklet.push` logger** (INFO) narrates every push: a start record
  with exact totals (groups, keys, bytes — computed upfront from the captured
  lengths), one record per group completion (done/total, bytes, per-group pack
  and PUT seconds, cumulative MB/s, ETA), a summary, and a commit record. The
  phase-A pull line moved from `ebooklet.utils` onto this logger. Opt in with
  e.g. `logging.getLogger('ebooklet.push').setLevel(logging.INFO)` plus a
  handler; see docs/ops.md "Monitoring a push".

### Fixed — quadratic group packing (the PRIMARY production bottleneck)
- **`pack_group` built its payload by appending to an immutable `bytes` object** —
  every append re-copied the whole buffer, so packing a 134 MB group did
  ~100 GB of memcpy (~60–100 s of single-core CPU). Present since format 2
  shipped in 0.10.0 and invisible to every test (tiny groups) and to the
  disk-focused benchmark; found by a pre-release end-to-end push of a real
  2 GB cfdb subset, whose progress records showed 60 s packs with the disk
  fully idle. Re-running the production arithmetic (857 members, 134 MB) puts
  this — not disk seeks — as the dominant term of the observed one-group-per-
  ~105 s cadence. Now a `bytearray` (amortized O(1) appends): a 90 MB group
  packs in ~0.1 s. A tripwire test guards the complexity.

### Added — compaction guards
- **`prune()`/`clear()` now raise `PushInProgressError` while a push is
  running** — a compaction would move/destroy the value bytes the push is
  reading (captured offsets die on prune/clear).
- **`ConcurrentCompactionError`** (new, raised by `push()` BEFORE its commit):
  the belt for out-of-band compactions the session guard cannot see (e.g. a
  direct booklet `prune()`). Nothing is committed, staged, or journal-cleared;
  any group objects already uploaded are invisible orphans (fsck sweeps them);
  the retry re-captures and converges. Both errors are `Error` subclasses and
  exported.

Phases 1 and 2 of the architecture-assessment roadmap, shipped as one release
(each phase's design dual-reviewed pre-implementation: `planning/phase{1,2}-design-brief.md`
+ the four `planning/phase{1,2}-review-*.md` reports). Phase 1 is the structural round
(journal, generational format 2, fsck, lock safety); Phase 2 is the pre-1.0
API round (typed exceptions, MutableMapping conformance, PushResult, offline
read mode, the ops guide).
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

### Added — typed exception taxonomy (Phase 2)
Every ebooklet-raised consumer-facing error now derives from `ebooklet.Error`
(new `errors` module, all classes exported at the package root). Existing
handlers keep working: the new classes keep their legacy parentage via dual
inheritance for this release.
- `ReadOnlyError(Error, ValueError)` — every "read only"/"not writable" guard
  (covers both flag='r' sessions and missing write credentials).
- `UUIDMismatchError(Error, ValueError)` — local file vs remote, and http vs
  S3 connection identity mismatches (previously bare ValueErrors that
  consumers had to string-match).
- `RemoteMissingError(Error, ValueError)` — read-open with no remote and no
  local file; RCG member registration of a nonexistent remote; `copy_remote`
  from a nonexistent source.
- `UnsupportedFormatError` and `GroupTooLargeError` re-parented under `Error`
  (still ValueErrors); `RemoteIntegrityError` gains `Error` and KEEPS its
  urllib3 `HTTPError` parentage.
- New `LockLostError(Error)` replaces the bare HTTPError at the two
  push-boundary lock verifications. Deliberately NOT an HTTPError: a lost
  lock is not connectivity, and connectivity handlers must not treat it as
  such. The lock-acquire timeout stays the builtin `TimeoutError`; argument/
  configuration validation stays builtin ValueError/TypeError (incl.
  `copy_remote`'s target-already-exists).
- New `OfflineError(Error)` — see offline read mode below.

### Changed — MutableMapping conformance + true clear() (Phase 2)
- **`del eb[key]` of a missing key raises `KeyError`** (was a silent no-op).
  "Missing" follows `in`: not in the remote index and not in the local file.
- **`update()` implements full dict.update semantics** — a mapping, an
  iterable of key/value pairs, and/or keyword arguments (was: one mapping).
- **`clear()` is a TRUE clear** (was: local cache eviction). Every key is
  journaled as a deletion and applied by the next push, which commits an
  atomically-empty database; until pushed, `changes().discard()` cancels it.
  Unpushed pending writes are dropped by a clear (deliberately, without a
  warning — clear means clear). Local cache eviction's replacement idiom is
  `prune(timestamp=<now>)` (journal-pending writes are never evicted).
- The dead `force_shutdown` parameter is removed from `close()`/`sync()`
  (it had no callers and no longer had an implementation to serve).
- `Change.update()` is renamed **`build_changelog()`** (the old name was
  easily confused with the MutableMapping `update()` on the ebooklet itself;
  it was only called internally).
- `push()` on a read-only session now raises `ReadOnlyError` (previously an
  AttributeError, because the remote-writability guard ran first against a
  reader session object that has no such attribute).

### Changed — `push()` returns `PushResult` (Phase 2)
`push()` now returns a `PushResult` (msgspec Struct) instead of
True/False/dict: `updated` (the remote changed), `failures` (per-key/group
upload failures as `'ExceptionClassName: message'` strings; pending changes
for failed entries stay journaled), and `bool(result)` = fully-successful
push that changed the remote. Semantics fixes ride along: the legacy
partial-failure dict was accidentally TRUTHY (`if push():` misread partial
failure as success — any failure is now falsy), and `updated` is precise
about partial pushes (an ordinary partial push HAS committed its successful
groups → True; a partial replacement push commits nothing → False). Commit
failures still raise (HTTPError / LockLostError) rather than returning — see
`docs/ops.md` for the two failure channels.

### Added — offline read mode (Phase 2)
`open_ebooklet(..., offline=False|'auto'|True)` and the same on `open_rcg`
(read-only: requires flag='r'; the session exposes an `.offline` property).
`offline=True` never touches the remote: it serves the existing local file
as-is; reads of values not materialized locally raise `OfflineError` (the
key exists — its value needs the remote; bulk reads raise ONE error naming
the keys before any fetch is attempted). `offline='auto'` opens online and
falls back to offline (with a UserWarning) ONLY on transport-level
unreachability (DNS/connect/timeout) — integrity/format/uuid faults and HTTP
status errors (e.g. bad credentials) still raise, so a broken remote is
never masked by stale local data. Offline covers the opened database only:
an RCG opened offline browses the catalogue, but member opens still need
connectivity.

### Added — operations guide
`docs/ops.md`: the recovery recipes in one place — partial-failure retry,
`force_push` after a failed commit, fsck usage, lost/stuck locks and the
force_lock age gate, `RemoteIntegrityError` triage, `flag='n'` replacement
guidance ("use 'c' unless you mean to replace"), offline mode, and the
format-1 → format-2 upgrade recipe.

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
review (`planning/phase1-review-claude.md`, F-1) and verified against 0.9.5.

## 0.9.5 (2026-07-12)

Phase 0 of the adopted architecture-assessment roadmap (see
`planning/architecture-assessment-synthesis.md`); design dual-reviewed pre-implementation
(`planning/phase0-design-brief.md` + the two `planning/phase0-review-*.md` reports).

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
