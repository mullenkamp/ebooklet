# Changelog

Notable changes to ebooklet. The format loosely follows [Keep a Changelog](https://keepachangelog.com/);
ebooklet does not promise SemVer — minor versions may change behavior.
Entries for 0.8.3 and earlier were reconstructed from commit history after the fact.

## 0.9.2 (202-07-09)

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
