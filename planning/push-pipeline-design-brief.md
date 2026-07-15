# Critical design review: pipelined push + progress logging (ebooklet 0.10.1, booklet 0.12.8)

**Date:** 2026-07-14 · **Author:** the maintainer of booklet/ebooklet, requesting a critical pre-implementation review of his own design. Nothing below is implemented yet — you are reviewing a design against the real source.

## Your task

Evaluate whether this design is sound and implementable as specified. Specifically:

- **Challenge the design's assumptions.** Every load-bearing claim is listed in §5 with the author's evidence — re-verify each against the actual source (line numbers are current as of today but re-check; do not take the author's word).
- **Probe for failure modes the author did not consider.** The listed questions are a floor, not a ceiling — the most valuable findings are the ones outside them.
- **Run experiments where useful.** You may create throwaway scripts/venvs under `/tmp` (the released packages are on PyPI: booklet 0.12.7, ebooklet 0.10.0; the local trees can also be imported read-only). Empirical checks of the offset-stability claims are explicitly encouraged.
- **Ground rules:** the repositories are read-only for this review — do not modify, create, or delete any file inside `~/git/booklet`, `~/git/ebooklet`, or any other repository. New files go under `/tmp` only. Do not open `ebooklet/tests/s3_config.toml` (routine git-ignored local machine config — out of scope).
- **Scope:** full source of `~/git/booklet` and `~/git/ebooklet`. Optional read-only context for the downstream call sites: `~/git/cfdb-repos/cfdb/cfdb/edataset.py`, `~/git/envlib-repos/envlib/envlib/catalogue.py`.

## 1. Background and the measured problem

booklet is a single-file key-value store (hash-bucket index, append-oriented data blocks, thread-locked file access). ebooklet layers an S3-synced remote on top: `push()` packs changed *groups* of keys into immutable generation objects and PUTs them, then commits via a single db-object PUT (the format-2 A/B/C/D protocol documented at `ebooklet/utils.py:848-871`).

Measured on the first production-scale push (20 GB, 149 groups of ~134 MB, `threads=10`, local file on a spinning HDD):

- Groups completed strictly one at a time, every ~105 s — effective concurrency 1, ~1.25 MB/s aggregate on an unsaturated uplink.
- Root cause: each phase-B worker (`upload_group`, `utils.py:517-540`) packs its group by calling `local_file.get_timestamp(key, include_value=True, decode_value=False)` for each of ~857 members — every call takes booklet's `_thread_lock` and performs one random ~157 KB read. Ten workers interleaving random reads thrash the disk head to ~9 reads/s, so packing produces ~1.4 MB/s of ready payloads and the network idles between PUTs.
- Additionally, `push()` emits no progress. The 20 GB push was monitored with external network tools and bucket listings.

Deliverables: **booklet 0.12.8** (one new public iterator + one property) and **ebooklet 0.10.1** (reworked phase-B reads, a read-concurrency gate, push progress logging, a compaction guard). No cfdb/envlib code changes.

## 2. Proposed design — booklet 0.12.8

**2a. `Booklet.locations()`** — new public generator on the base class (sibling of `timestamps()`, `booklet/main.py:337`):

- Yields `(key, timestamp_int_us, value_offset, value_len)` for every live user key, **header-only** — after reading each 25-byte block header + timestamp + key it seeks past the value bytes instead of reading them. Excludes the metadata key and reserved-slot keys, skips dead blocks (`next_ptr == 0`), exactly like the existing iterators (`iter_keys_values`, `booklet/utils.py:545-618`, including the relocated-index two-region layout after a reindex).
- The offset math already exists verbatim in `mmap_get_value` (`utils.py:644`): `value_offset = data_block_pos + 25 + ts_bytes_len + key_len`.
- Sync-first: unflushed writes sit in `_buffer_data` with *predicted* offsets and unwritten index entries, so `locations()` calls `sync()` before walking (the same discipline `get()` applies per-key at `main.py:425-426`). Per-step locking + the `_mutation_count` iterator guard, like `_iter_locked` (`main.py:241-278`).
- Works in both open modes (mmap for read-only opens, file reads for write-mode opens — write mode has no mmap, `utils.py:1391-1396`).
- `FixedLengthValue.locations()` raises `NotImplementedError` (the reserved-slots precedent, `main.py:1111-1121`; fixed blocks have different framing and no per-item timestamp).
- Timestamp position is `None` for files created with `init_timestamps=False`.

**2b. `Booklet.compaction_count`** — read-only property exposing the existing `_compaction_count` (bumped by `prune()` at `main.py:633` and `clear()` at `main.py:716`; handle-scoped, starts at 0 per open). External holders of captured offsets snapshot it and re-check to detect invalidation.

The documented validity contract: **value blocks are append-only** — a captured `(offset, len)` stays valid across `set`/overwrite/delete/auto-reindex and is invalidated only by `prune()`/`clear()`. Author's evidence: overwriting appends a new block and repoints the chain, only zeroing the *old block's* 6-byte next_ptr (`update_index`, `utils.py:995-1002`); delete likewise zeroes a next_ptr (`assign_delete_flag`, `utils.py:866-900`); auto-reindex appends a fresh bucket index and rewrites only next_ptrs (`reindex`, `utils.py:191-266`); `prune_file` compacts/moves blocks (`utils.py:1052-1194`) and `clear` truncates (`utils.py:1029-1049`); reserved-slot writes route through the ordinary append path (`set_reserved` → `write_data_blocks`, `main.py:165`).

## 3. Proposed design — ebooklet 0.10.1

**3a. Capture during the changelog pass.** `create_changelog` (`utils.py:678-766`) already sweeps every local block header via `local_file.timestamps()` (`:703`, `:750`). It switches to `local_file.locations()` and builds an in-memory side map `loc_map = {key: (ts, offset, len)}` while writing the changelog exactly as today — zero additional IO, and entries arrive in file order (ascending offset). The skew-normalization branch (`:736`, in-place `set_timestamp` after the main sweep) refreshes the affected key's `loc_map` entry in the same block. The map is *not* persisted (the changelog file's fixed 14-byte values cannot hold 6-byte local offsets, and a retried push rebuilds the changelog anyway). `create_changelog` returns `(changelog_path, loc_map)`; `Change.push` passes the map into `update_remote`.

**3b. `update_remote` grouped path** (`utils.py:885-1030`) consumes the map:

- Local-side group membership comes from `loc_map` keys (replacing the `local_file.keys()` pass at `:901-906`). The remote-index union loop (`:918-949`) is unchanged except its local-timestamp comparisons read from `loc_map` instead of per-key `get_timestamp` calls.
- **Phase A pulls hand bytes back in memory:** `get_remote_group_values` additionally returns `{key: (ts, bytes)}` per group — it still materializes pulled values into `local_file` as today (read-your-writes), but the pack no longer re-reads from disk what was just downloaded. The `b''` empty-member branch (`:949`) records into the same structure.
- Lost keys = group members with no `loc_map` entry and no pulled bytes — semantically identical to today's `get_timestamp(key) is None` loop (`:979-997`), same warning and index cleanup.
- **Phase B keeps the fused-worker topology** (one executor, `max_workers=remote_session.threads`, one task per group, workers return `(error, offsets)` and never raise, the `as_completed` bookkeeping loop at `:1015-1028` essentially verbatim). The reworked worker:

  ```
  with _pack_gate:                       # threading.BoundedSemaphore(push_packers), default 1
      open a private fd on the local file (plain open(path, 'rb'), per task;
          deliberately NOT os.pread — POSIX-only; a private fd + seek/read has
          no shared-position hazard and is portable)
      read the group's members at their captured offsets, in offset order
          (already sorted from the capture); merge the phase-A in-memory bytes;
          verify each read returns exactly value_len bytes
      pack_group(entries)                # unchanged (utils.py:202-229); GroupTooLargeError intact
      check local_file.compaction_count == snapshot    # detects mid-push compaction
  PUT to group_obj_key(gid, gen)         # outside the gate — up to ~threads concurrent PUTs
  ```

- **Read gate rationale (default 1):** the gate serializes only pack-vs-pack — pack/upload overlap is unaffected (while one worker reads, up to nine others are PUTting already-packed payloads). Width 1 gives the single-elevator disk pattern; one sorted sweep (~15-20 MB/s expected) outruns any plausible uplink. `push_packers` is a new `open_ebooklet`/`open_rcg` kwarg (validated ≥ 1); setting it to `threads` recovers ungated behavior for SSD/RAID storage. A pre-release benchmark compares widths 1/2/3/10 and may adjust the default.
- **Portability fallback:** on Windows, a write-mode booklet's `portalocker` lock is `LockFileEx` — *mandatory* byte-range locking, so a second read handle can be denied. On OSError from the fast-path open/read, the worker falls back for that group to today's locked `get_timestamp(include_value=True)` reads (one warning per push). POSIX locks are advisory; Linux/macOS unaffected.
- **Timestamp tightening:** staged index entries (`:1024`) take their timestamp from `loc_map`/pulled bytes instead of the post-upload `get_timestamp` re-read at `:1021` — the packed ts and the staged ts become identical by construction.
- RAM ceiling unchanged: ≤ `threads` materialized payloads (each worker holds at most one). Payloads stay fully materialized — SigV4 signed-payload PUTs need the body SHA-256 before the first byte; unsigned-payload and aws-chunked signing were considered and rejected (provider portability, retryability of streamed bodies). Multipart upload rejected as unnecessary below ~1 GB/object.
- The ungrouped per-key legacy path (`:1032-1049`) is untouched.

**3c. Compaction guard.** New `PushInProgressError` (in `errors.py`, Error + RuntimeError parentage). `Change.push` sets a `_push_active` flag around `update_remote` (try/finally); `EVariableLengthValue.prune()` (`main.py:754`) and `clear()` raise while it is set. Belt: the per-worker `compaction_count` check above — on mismatch the push fails **before phase C** (the coordinator raises pre-commit; nothing is staged, committed, or journal-cleared; any generations already PUT are invisible orphans, the format's standard crash-before-C story, sweepable by fsck).

**3d. Progress logging** (no API change). Dedicated logger `logging.getLogger('ebooklet.push')`, all INFO: the existing phase-A pull line (`:952`) moves onto it; a start record (`{n} groups, {k} keys, {total} bytes` — exact totals from `loc_map` lengths + the pack-format per-entry overhead); one record per group completion (done/total, cumulative MB, cumulative-mean MB/s, ETA, plus per-group pack-seconds and PUT-seconds — the future tuning evidence); a final summary and a post-commit line. Failures log at WARNING. Cumulative-mean rate, no windowed rates. `PushResult` (`main.py:62-87`: `updated`, `failures`, `__bool__`) is untouched.

## 4. Alternatives considered and set aside (challenge these calls too)

- **Packer pool → bounded queue → uploader pool** (an earlier draft): more overlap on paper, but a higher RAM ceiling (~packers+queue+threads payloads vs ≤ threads), and it requires a shutdown/backpressure protocol whose first draft already contained an unbounded-submit defect. The fused shape + gate was judged equal-throughput in both bottleneck regimes at a fraction of the complexity. If you believe the queue topology (or another shape) is materially better, say so with reasoning.
- **Standalone bulk-lookup API** (`locations(keys) -> dict` resolved by per-key index walks): ~2-3 random reads per key ≈ 20-35 minutes of serialized index walking for a 128k-key cold-cache push — superseded by capturing during the changelog sweep that already runs.
- **Streaming/multipart PUTs, s3func changes, adaptive gate-width heuristics:** out of scope this round (rationales above).

## 5. Load-bearing claims to verify (the author's homework — please re-check it)

- **C1 — append-only offsets:** no code path other than `prune()`/`clear()` moves or overwrites a stored value's bytes. Verify against `update_index` (`booklet/utils.py:963-1026`), `assign_delete_flag` (`:866-900`), `reindex` (`:191-266`), `write_data_blocks` (`:903-940`), `set_timestamp` (in-place ts rewrite at a *fixed* position inside the block — `:454-468`; does not move the value), `set_reserved`/`set_metadata` (append path). Anything missed?
- **C2 — sync-first sufficiency:** after `locations()` syncs under the thread lock, every returned offset is on-disk-valid and readable through an independent fd (files are opened `buffering=0`, `utils.py:1279/1378`; OS page cache makes flushed writes visible to other fds without fsync). Is there any window where the index references a predicted offset whose bytes are not yet written?
- **C3 — capture-to-use staleness:** between the capture (changelog build) and the reads (phase B), the only local-file mutations are the skew-normalization `set_timestamp` calls (handled in the map), phase-A pull materializations (handled in memory), and reserved-slot journal/remote-state persists (append-only). Is anything else mutating the local file inside `Change.push`? Check `main.py:212-303` end to end, including the replace-pending purge (`:252-263` — runs *before* the changelog build; confirm).
- **C4 — iterator fidelity:** the new scan yields exactly the live user keys — no deleted/superseded blocks, no reserved/metadata keys, correct behavior after a reindex has relocated the bucket index, correct on files whose write buffer held entries at scan time.
- **C5 — lost-keys equivalence:** "no `loc_map` entry and no pulled bytes" is semantically identical to today's `get_timestamp(key) is None` test at `:987` for every reachable state (including keys deleted locally but present in the remote index, and the `b''` empty-value members).
- **C6 — worker error contract:** every failure inside the reworked worker (short read, OSError, GroupTooLargeError, compaction mismatch, fallback failure) surfaces through the existing `(error, offsets)` return and lands in `PushResult.failures` — with the single exception of the compaction mismatch, which deliberately aborts the whole push pre-commit. Is that exception's routing (raise vs per-group failure) the right call?
- **C7 — gate semantics:** `BoundedSemaphore(1)` around read+pack cannot deadlock with the executor (workers = threads ≥ 1), cannot starve PUTs, and preserves today's behavior when `push_packers >= threads`.
- **C8 — Windows fallback:** the mandatory-lock denial is detected reliably (which exception surfaces?), the fallback path is correct, and the fast path cannot silently read wrong bytes on any platform.
- **C9 — pulled-bytes RAM:** phase-A pulled values are held in memory from pull completion until their group packs. For the common cases (fresh push: zero pulls; incremental push: sparse pulls) this is small, but a stale local file pushing many remote-heavy groups could hold a large pulled volume. Is a cap/spill needed, or is falling back to booklet reads for pulled keys (page-cache-hot, they were just written) the better design?
- **C10 — progress/thread-safety:** logging from worker threads plus a counters-under-lock helper introduces no race with the bookkeeping loop; record counts and byte totals are exact.
- **C11 — semantic invariance:** for any sequence of operations, the remote state after the new push (group objects' content, index entries, manifest, journal state) is content-identical to today's — entry *order inside* a group object may change (offset order vs set order); confirm nothing depends on it (readers resolve via per-key offsets returned at pack time).
- **C12 — hermetic testability:** the design's test list (content-identical push, caplog record counts, guard tests, failure injection, fallback exercise) is achievable on the existing `fake_s3` harness without live S3.

## 6. Deliverable

1. **Verdict:** implementable as specified / implementable with changes (list them) / not implementable (why).
2. **Findings**, ordered by severity (CRITICAL / MAJOR / MINOR / NOTE), each with: file:line evidence, a concrete failure scenario (inputs/state → wrong outcome), and a suggested resolution.
3. **Confirmations:** which of C1-C12 you verified and how (source reading vs experiment), including any you could not confirm.
4. **Experiments run** (if any): what, where (under /tmp), and what they showed.
5. A statement that no repository files were modified during the review.
