# Critical design review: ebooklet local-cache reconciliation (pre-implementation)

I'm the author of this stack, requesting a rigorous independent design review of a fix
BEFORE I implement it. Please challenge my assumptions, verify every claim against the real
source code, run small experiments where useful, and probe for failure modes I did not think
to list. Judge the design by the code, not by my description of it.

Ground rules: you may read anything under `~/git/ebooklet` and `~/git/booklet`. Please do
not modify, create, or delete any file inside those repositories. Throwaway scripts/tests
go under `/tmp` only. `~/.envlib` is git-ignored user configuration — out of scope, don't
open it. Work offline against the local repos (from `~/git/ebooklet`, `uv run python` has
the package importable).

## Context

ebooklet (`~/git/ebooklet`, 0.10.1) syncs a local booklet key-value file with an S3 remote
(local file = cache/working copy; remote = object groups + a remote index; push is explicit).
booklet (`~/git/booklet`, 0.12.8) is the underlying single-file KV store. A live defect was
confirmed today: **remote key deletions never reach warm local caches.** A reader that
previously materialized a key's value serves it forever — `keys()` (ebooklet/main.py:635-647)
yields local keys unconditionally, `__contains__` (740) consults the local file, and
`_load_item` (1144-1185) serves the local value when `remote_index.get(key)` is `None`
(a deleted entry is indistinguishable from "nothing newer to fetch"). The only cleanup path,
`_resolve_missing` (1015-1107), fires solely when a remote fetch returns missing — which
never happens for an already-materialized key. I also believe there is a second hazard:
`create_changelog` (ebooklet/utils.py, ~852) enters any index-absent local key into the push
changelog as a NEW write (remote-ts 0), so a warm WRITER would silently re-publish keys that
were deleted remotely.

## Proposed design (the review target)

New helper `reconcile_local_with_index(local_file, remote_index, journal, prev_synced_ts)`
in ebooklet/utils.py. One pass over `local_file.keys()` collecting candidates; deletions
applied AFTER the scan completes (booklet raises RuntimeError on delete-during-iteration).
A key is deleted only if ALL of:

1. `key not in utils.reserved_key_strs` — metadata is never a remote-index entry in storage
   format 2, so without this exclusion the scan would null the metadata on every pull;
2. `key not in remote_index` — the fresh index no longer claims it;
3. `key not in journal.written` — the main.py:1072-1076 invariant (no remote-driven path may
   delete a journal-pending local value);
4. local value timestamp `<= prev_synced_ts`, where `prev_synced_ts` is
   `RemoteState.remote_ts` captured BEFORE `update_committed` runs — the commit timestamp of
   the LAST remote index this local file ingested. If either timestamp is None → keep.
   Rationale: the journal alone is insufficient — journal.py's docstring documents that
   booklet auto-flushes data blocks mid-bulk-load, so durable unpushed writes can exist while
   the journal (persisted at sync boundaries) is stale; those are index-absent, and
   journal-only reconciliation would delete them (the push changelog's timestamp-diff leg
   exists to rescue exactly those). Remotely-sourced local values carry the timestamp of the
   index entry they were pulled from, hence <= the ingest commit ts; post-sync local writes
   are newer. Residual loss window I believe remains: crash-with-stale-journal AND
   skewed/backdated timestamp AND a foreign push before reopen.

`prev_synced_ts` deliberately does NOT use `local_file._file_timestamp`: the open path never
calls `_set_file_timestamp` (only push and pull do), so a read-only cache's file stamp stays
creation-time-stale and the guard would make the fix silently inert for read-only caches —
the primary live case. `RemoteState.update_committed` + persist DO run on every open-path
fetch (main.py:475-479).

Call sites:

A. `_pull_remote_index` (main.py:933-1012): after the journaled-delete replay (990-992),
   before `update_committed` (997), under `self._index_lock`. Runs on every successful fresh
   fetch including `force=True` from `Change.discard` (safe, I claim, because discarded
   writes were already removed from the local file at main.py:189, so an in-sync force pull
   has no candidates).
B. The open path `_init_common` (main.py:466, 475-494): `utils.get_remote_index_file`
   (utils.py:367) gains an explicit `fetched` bool return; capture `prev_synced_ts` right
   after `RemoteState.load` (475), before `update_committed` (477); reconcile after the
   journaled-delete replay (492-494) ONLY when `fetched` is True. Cached-index reuse does
   not reconcile — a reused index carries no new remote truth; pre-fix stale residue heals
   at the next remote change or explicit `pull()`.

Knock-ons I claim: reconciling at writer-open closes the changelog re-publish hazard (a
writer's open always ingests a fresh index and holds the remote lock thereafter). Offline
sessions and `_index_fetch_suppressed` never reconcile. `flag='n'` recreates an empty local
file → empty scan. Logging: one logger.info with count + up to ~50 keys.

Planned tests (judge sufficiency): warm-key pull() convergence (per-key AND grouped);
re-open convergence (flags 'r' and 'w'); journal-pending write survives; UNJOURNALED
crash-window write survives (written via local_file.set directly); metadata survives; no
re-appearance across pulls; push-does-not-republish (pre-fix failing); reader whose only
syncs were open-path fetches reconciles on reopen; discard() force-pull loses nothing;
offline reopen serves the stale cache without error.

## Review tasks

1. Verify EVERY factual claim above against the source (file:line given where I have one;
   find the truth where I don't). Flag anything wrong or imprecise.
2. Stress-test the timestamp discriminator: construct a concrete sequence where a value that
   was NOT deleted remotely passes all four checks and is removed locally. Consider crash
   windows, clock skew, every `set_timestamp` use in ebooklet/booklet, the exact timestamp
   a materialized remote value is stored with (find the code), push's own timestamp rewrites
   (create_changelog's skew branch), replace/copy_remote paths, num_groups changes,
   generational GC.
3. Stress-test the reverse: can a remotely-deleted key survive reconciliation and keep being
   served (fix incomplete)? Walk every read/enumeration path: `load_items`, `get_timestamp`,
   `timestamps()`, iterators, the RemoteConnGroup subclass, cfdb's EDataset usage.
4. The two call sites: is any other index-ingest path missed (open under different flags,
   discard, `_resolve_missing`'s re-pull, replace_remote, copy_remote, fsck)? Is holding
   `_index_lock` sufficient against concurrent readers in other threads of one process?
5. The fetched-only rule at open: could cached-index-reuse reconciliation ever be REQUIRED
   for correctness (not just hygiene)?
6. Interactions: journal deletes set, meta_pending, replace_pending, num_groups migration,
   `Change.discard`'s force pull, prune().
7. Anything I did not think of. Spend real effort here — nonconforming producer input,
   partial failures, odd-but-legal call orders.

Experiments encouraged: ebooklet/tests/fake_s3.py provides an in-memory fake remote
(FakeS3Connection, make_writer/make_reader) — copy what you need to /tmp to empirically
confirm the defect, the re-publish hazard, and the timestamp semantics of materialized
values.

## Deliverable

A markdown report: (1) verdict — implementable as designed / implementable with changes /
needs redesign; (2) findings ordered by severity (CRITICAL/MAJOR/MINOR/NOTE), each with
concrete evidence (file:line or experiment output) and a specific recommended change;
(3) author claims you verified as correct; (4) what you checked and found clean.
