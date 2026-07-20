# Claude review report — ebooklet local-cache reconciliation (plan stage)

Run 2026-07-20 as a fresh-context blind subagent against the adversarial twin of the
Gemini brief, same scope (`~/git/ebooklet` @ 0.10.1, `~/git/booklet` @ 0.12.8,
`~/.envlib` excluded). The session ran read-only (no /tmp experiments executed); every
finding is pinned to source with file:line. Verbatim.

---

# Adversarial design review: `reconcile_local_with_index` (ebooklet deletion-propagation fix)

## 1. Verdict

**Implementable with changes.** The core mechanism is sound: the four-check candidate rule
is conservative in the right direction, both call sites are correctly chosen, the
`fetched`-only rule at open is provably sufficient, and the design closes both the
read-side staleness defect and the push-resurrection hazard for the mainstream cases. Two
things need to change or be consciously accepted before implementation: the timestamp
discriminator's exposure to *user-supplied* timestamps (a first-class ebooklet API, not
just "clock skew"), and the reconcile scan's behavior under concurrent same-process
mutation (new `RuntimeError` failure mode + a convergence-defeating race). One rationale
claim in the design is factually wrong (harmlessly).

## 2. Findings

### MAJOR-1 — The timestamp discriminator treats user-supplied timestamps as clock anomalies; they are a first-class API

The design frames check 4's residual as "crash-with-stale-journal AND skewed/backdated
timestamp". But explicit timestamps are a documented public feature at BOTH layers:
- `EVariableLengthValue.set(key, value, timestamp=None, ...)` — main.py:712;
  `set_timestamp` — main.py:695.
- booklet `set(..., timestamp=...)` accepts any int/str/datetime with no range validation
  (booklet/main.py:599-627, `make_timestamp_int` booklet/utils.py:269-302).

Two concrete consequences:

**(a) Loss direction.** For any consumer that stamps domain times (observation dates,
etc. — plausible in this stack; I could not read cfdb, which is outside my scope —
**verify whether cfdb/EDataset ever passes explicit timestamps**), *every* crash-window
write (durable via booklet's auto-flush, journal stale — journal.py:13-17) carries a
"backdated" stamp by construction. The acknowledged narrow residual becomes the common
case: unjournaled + index-absent + ts ≤ prev → destroyed at the first reconcile after any
foreign push. The changelog's timestamp-diff rescue leg (utils.py:852-854) never gets a
chance — reconcile deletes the key before any changelog is built.

**(b) Leak direction.** A key pushed with a *future* timestamp materializes locally with
that stamp (pulled values are stamped with the remote index entry's ts — verified at
utils.py:517-519 per-key, utils.py:746-753 grouped, utils.py:686 recovery path). After
remote deletion: index-absent, journal-clean, but ts > prev_synced_ts forever → the fix is
inert, the key serves forever, **and** the push-resurrection hazard survives for it (it
re-enters `create_changelog` as NEW with remote-ts 0).

**Recommended change:** add an *old-index-membership* leg to the discriminator. A key
claimed by the PREVIOUS ingested index but absent from the fresh one (and journal-clean)
was remotely deleted — delete it regardless of its local ts; a key in *neither* index is a
local-origin write — keep it regardless of ts. This is exact provenance, immune to both
directions above. Implementation cost: at call site A, snapshot `key in old_index` for
local keys *before* the handle swap (main.py:980-985); at call site B, consult the
pre-overwrite sidecar before `get_remote_index_file` rewrites it in place (utils.py:401
writes over `dest_path` directly — the open path would need a fetch-to-tmp-then-swap like
pull's, or a membership snapshot first). Keep the ts leg **only** as the one-time healer
for pre-fix stale residue (whose deletion predates the last ingested index and is
therefore invisible to the membership test), ideally writer-logged rather than silent. If
the author rejects the added complexity, at minimum: document both directions and add the
two tests in NOTE-9.

### MAJOR-2 — Concurrent same-process mutation: a new pull/open failure mode plus a convergence-defeating race

booklet invalidates open iterators on ANY mutation — including `set()` from another
thread — raising `RuntimeError('booklet mutated during iteration')` at the next step
(booklet/main.py:266-278, docstring 285-291). Two concrete consequences the design doesn't
address:

**(a) The reconcile scan itself can raise.** `load_items` dispatches fetch workers under
`_index_lock` but the workers' `local_file.set(...)` calls land *after* the lock is
released (main.py:841-911: the `as_completed` drain at 913 is outside `_index_lock`). A
`pull()` in another thread that starts its reconcile scan while workers are materializing
values will get `RuntimeError` mid-scan, and since `_resolve_missing` re-raises pull
failures (main.py:1047-1053), a plain `get()` in one thread racing a `load_items` in
another can now fail with `RuntimeError`. Pre-fix, `_pull_remote_index` never iterated the
local file, so this failure mode is **new**. Recommendation: wrap the candidate-collection
scan in a retry-once-then-skip-with-log; a skipped reconcile is exactly the pre-fix status
quo and converges at the next fresh fetch.

**(b) Re-materialization race defeats convergence permanently.** A `load_items`/`_load_item`
worker that captured a remote index entry from the OLD handle (pre-swap) can complete its
`local_file.set(key, value, old_entry_ts)` *after* reconcile deleted that key. The key is
then local-present, index-absent again — and since a subsequent reconcile only runs on a
fresh fetch, and the freshness gate (utils.py:309-326) skips an unchanged remote, an idle
remote means the stale key is served indefinitely, exactly the original defect. It heals
only at the next actual remote change. Given ebooklet's documented partial thread model
(Change.pull docstring, main.py:122-129) this may be acceptable — but it must be a
*decision*, documented; or close it by having workers re-check current-index membership
under `_index_lock` before `set()`.

Also note: the reconcile's post-scan deletions bump `_mutation_count` (booklet
`__delitem__`, main.py:746), so a user iterating `keys()`/`items()` in another thread
while a pull reconciles now reliably gets `RuntimeError`. Pre-fix pulls could already do
this (a dirty `journal.persist` → `set_reserved` bumps at booklet/main.py:164;
`refresh_local_metadata` → `set_metadata` bumps at booklet/main.py:96), but it becomes
systematic. Document in `pull()`'s docstring.

### MINOR-3 — The check-1 rationale is factually wrong (the check itself is harmless belt); the metadata analog of the defect remains open

The design claims: "without [the reserved-keys check] the scan would null metadata on
every pull." False: booklet's variable-length iterators (`keys`/`items`/`values`/
`timestamps`/`locations`, file and mmap variants) all skip `reserved_key_bytes` — which
includes the metadata key — at booklet/utils.py:571, 645, 801, 866 (`reserved_key_bytes`
defined at booklet/utils.py:363). `local_file.keys()` can never yield the metadata key or
the journal/remote-state slot keys, so check 1 is unreachable. Keep it as belt (cheap,
guards a future booklet change), but fix the rationale.

The flip side the author missed: because the scan never sees metadata, **remote metadata
deletion still doesn't propagate**. `refresh_local_metadata` early-returns when the
fetched payload's meta section is absent (utils.py:447-448), so a remote whose metadata
was removed (e.g. a replacement push, which deliberately embeds no section —
utils.py:985-989) leaves warm caches serving the old metadata forever. The only nulling
path is `_resolve_missing`'s (main.py:1078-1082), which needs a fetch-404. This is the
metadata twin of the key defect and is NOT closed by this design — file it as its own
backlog item.

### MINOR-4 — "Pre-fix stale residue heals at ... explicit pull()" is wrong

`Change.pull()` on an *unchanged* remote early-returns at the freshness gate
(main.py:966-968) before the fetch, so no reconcile runs. Residue heals only on (a) an
actual remote change, or (b) a reopen whose file stamp is stale — which, since the open
path never stamps (verified: `_set_file_timestamp` is called ONLY at main.py:1012 (pull)
and utils.py:1427 (push)), covers every open-path-only reader but NOT a long-lived session
or a cache that last synced via `pull()`. Either correct the claim/docs, or give
`Change.pull` a `force` passthrough for a deliberate heal.

### MINOR-5 — Empty-index mass deletion: correct but deserves a louder guard

A legitimately emptied remote (`clear()` + push) makes reconcile delete every materialized
local key — correct semantics, but the blast radius of a *wrong* deletion pass is now "the
whole cache", where the pre-fix failure mode was merely staleness. The truncated/
corrupt-fetch path is adequately guarded (payload magic/version checks utils.py:106-117; a
short index body fails `open_remote_index` and the pull aborts *before* the swap,
main.py:973-976), and a torn-down remote never reconciles (`fetch_remote_index` returns
not-fetched when `initialized` is False, utils.py:395-396; uuid changes raise
`UUIDMismatchError` before any ingest, utils.py:319-320). Still: escalate the planned
`logger.info` to `warning` when the deletion count exceeds some fraction of local keys, so
a surprise mass-purge is visible in logs.

### NOTE-6 — Pre-existing freshness-gate clock-skew hole limits the fix (and worse)

The whole mechanism sits behind `remote_session.timestamp > local_file._file_timestamp`
(utils.py:323). A foreign push whose commit ts (pusher clock) is ≤ the local stamp is
never fetched → never reconciled. Worse, independently of this fix: a warm writer in that
state pushes its own sidecar index bytes, whose entries still reference the foreign push's
GC'd generations — a stale-writer-push corruption scenario. Cross-machine sequential
writers with skewed clocks only; single-producer setups (the envlib commons) are immune.
Pre-existing, out of scope, but the same "wall-clock as truth" root cause as MAJOR-1 —
worth a backlog entry.

### NOTE-7 — Implementation details worth adopting

- Use one `timestamps()` (or `locations()`) pass instead of `keys()` + per-key
  `get_timestamp` — it yields (key, ts) in the same single header scan
  (booklet/main.py:337-378) and halves the point lookups. `init_timestamps=True` is the
  default and ebooklet depends on it everywhere.
- The scan runs on **every open** for open-path-only readers (since open never stamps,
  their sidecar refetches each open — see MINOR-4). O(local-file headers + one sidecar
  hash lookup per local key): fine at envlib scale, measurable for multi-million-key
  caches. Acceptable; just be aware it's per-open, not per-remote-change.
- The post-scan deletion loop is required and correctly designed: deletions during
  iteration raise (booklet/main.py:272-273), and each `del` is safe post-scan
  (`__delitem__` pre-syncs only if buffered — booklet/main.py:737-738 — and the scan's
  `_iter_locked` pre-synced already).
- `get_remote_index_file`'s current `fetched_manifest is not None` proxy is actually
  correct even for per-key remotes (a fetched empty manifest is `{}`, not None —
  utils.py:129); the explicit `fetched` bool is a clarity refactor, and main.py:466 is its
  only caller. Fine.

### NOTE-8 — Semantics to pin deliberately: write-vs-delete conflict resurrection

A journaled pending write for a key another client deleted remotely survives reconcile
(check 3) and the next push re-creates the key — likewise an unjournaled crash-window
write with a healthy timestamp. That is last-writer-wins conflict resolution toward the
write, consistent with the read-your-writes gates (main.py:855-858, 1148-1151).
Reasonable — but make it an asserted test, not an accident.

### NOTE-9 — Test-matrix sufficiency

The planned matrix is good and covers the mainstream paths. Add:
1. **Future-stamped key** deleted remotely → currently survives (fix inert) — pin whatever
   behavior you decide (MAJOR-1b).
2. **Backdated unjournaled crash-window write** + foreign push + reopen → pins the loss
   window (or the membership-leg fix; MAJOR-1a).
3. **Replacement convergence**: warm reader after another client's `flag='n'` re-push
   (same uuid — init_local_file keeps it, utils.py:339) sees old keys vanish. This is a
   real coverage gain of the fix: replacements currently strand warm caches identically.
4. **`prev_synced_ts` None** (pre-0.10 local file with no slot 2) → reconcile is a no-op.
5. **RemoteConnGroup flavor** at least once (envlib catalogue is an RCG; subclass verified
   to add nothing to the read paths — main.py:1367-1475).
6. **Legacy stale `_metadata` entry in the remote index** (the load_items comment case,
   main.py:850-852) → reconcile untouched by it.
7. **Journaled-write-vs-remote-delete conflict** → key survives and re-pushes (NOTE-8).
8. A concurrency smoke test (or at least the retry-skip behavior of MAJOR-2a, if adopted).
9. Run the cfdb + envlib suites against the patched stack — EDataset's
   `in`/`get`/`load_items` reliance is outside what I could read here.

## 3. Author claims VERIFIED correct

1. `keys()` yields local keys unconditionally — main.py:639-643.
2. `__contains__` consults the local file — main.py:740-744.
3. `_load_item` serves local when `remote_index.get(key)` is None — main.py:1153-1158 →
   `check_local_vs_remote` returns None for `remote_time_bytes is None`
   (utils.py:776-777) → falsy → local serve. Deleted is indistinguishable from in-sync.
4. `_resolve_missing` fires only via `MissingRemoteObject` markers, which require a value
   fetch, which a materialized key never triggers — main.py:919-928, 1181-1182.
5. `create_changelog` enters any index-absent local key as NEW with remote-ts 0 —
   utils.py:852-854; the resurrection completes through both push modes (grouped:
   key→affected group→loc_map→pack→staged index entry, utils.py:1113-1131, 1318-1321;
   per-key: utils.py:1348-1361).
6. booklet raises `RuntimeError` on mutation during iteration; post-scan deletion is
   mandatory — booklet/main.py:266-278.
7. The main.py:1072-1076 invariant (no remote-driven path deletes a journal-pending value)
   exists as stated — main.py:1071-1077.
8. Journal insufficiency: booklet auto-flushes data blocks mid-bulk-load while the journal
   persists at sync boundaries — journal.py:13-17; flush-on-full-buffer confirmed in
   `write_data_blocks*` (booklet/utils.py:2046-2049); `record_write` does not persist
   (main.py:712-724 never calls `journal.persist`).
9. Pulled values are stamped with the remote index entry's timestamp — utils.py:517-519,
   746-753, 686 (and per-key object metadata ts equals the index-entry ts by construction,
   utils.py:948-949 vs 1361).
10. `prev_synced_ts` must not be `_file_timestamp`; the open path never stamps —
    `_set_file_timestamp` call sites are exactly main.py:1012 (pull) and utils.py:1427
    (push).
11. `RemoteState.update_committed` + persist run on every open-path fetch —
    main.py:476-479.
12. Call site A's context: replay at main.py:990-992, `update_committed` at 997, all under
    `_index_lock` (RLock, re-entrant from `_resolve_missing`).
13. `discard()` force-pull safety: written keys are removed from the local file at
    main.py:189 before the pull; cancelled deletes were locally removed at `__delitem__`
    time (main.py:1221-1222); surviving journaled deletes are re-replayed onto the fresh
    index (990-992). No candidates. Also: a `discard` session holds the write lock, so the
    force-pulled index cannot carry foreign changes.
14. Offline and `_index_fetch_suppressed` sessions never reconcile — `_pull_remote_index`
    early-returns for suppressed (945-946) and raises `OfflineError` at
    `_load_db_metadata` (954, remote.py:650) for offline; the open path can't fetch
    (`fetch_remote_index` gates on `initialized`, utils.py:395).
15. `flag='n'` recreates the local file → empty scan — utils.py:337-344.
16. Writer-open always ingests fresh remote truth when foreign truth exists:
    `fetched=False` at a writer open ⟺ remote ts ≤ own stamp ⟺ no foreign push since the
    writer's own last push/pull (modulo the NOTE-6 skew hole) — so open-time reconcile
    does close the `create_changelog` resurrection hazard for sane clocks.
17. The `fetched`-only rule at open is sound (task 5): the invariant "every
    `fetched=False` open serves a sidecar whose ingest ran reconcile (post-fix)" holds
    because the stamp advances only at pull:1012 (after `remote_state.persist` at 1007 — a
    crash between leaves the stamp old, forcing a refetch) and at push (own session; no
    foreign deletions possible under the lock). The `elif` crash-window branch
    (main.py:480-487) is reachable only after this session's own committed push (stamp =
    own commit ts), where the journal replay — not reconcile — is the correct and
    sufficient repair for the un-applied staged sidecar. Cached-index-reuse reconciliation
    is never *required*; it would only re-run a pass that already ran.

## 4. Checked and found clean

- **No missed index-ingest path** (task 4): all ingests go through `get_remote_index_file`
  (only caller: main.py:466) or `fetch_remote_index` inside `_pull_remote_index`
  (973-976). `_resolve_missing` and `discard` both route through `_pull_remote_index`
  (site A). `copy_remote`/`delete_remote` touch no local state (main.py:1317-1334). `fsck`
  is a standalone remote-orphan sweeper with no local-cache interaction (fsck.py:53, 168).
  The `fetch_remote_state` elif ingests manifest/meta only, never the index body —
  reconciling there against the stale sidecar would be wrong, and correctly isn't done.
- **Journal interactions** (task 6): pending deletes can't be candidates (locally removed
  at `__delitem__`, replayed out of the index); `meta_pending` untouchable (scan never
  sees metadata); `replace_pending` sessions — 'n' opens scan an empty file; crashed-'n'
  'w'-recovery over v2 protects journaled replacement writes via check 3 and only exposes
  the same MAJOR-1a crash+skew window; v1 recovery is fetch-suppressed. `num_groups`
  adoption at pull (959-963) is orthogonal (reconcile is membership-based, not
  group-based).
- **prune()**: evicted keys stay index-present → not candidates;
  `keep_keys=journal.written` (main.py:803) is consistent with check 3; a reconcile delete
  never moves bytes (append-only delete flags), so a concurrent push's captured offsets
  stay valid — and pull-during-own-push can't fetch anyway (lock holder ⇒ remote
  unchanged).
- **Every read/enumeration path converges post-reconcile** (task 3): `keys`/`__iter__`/
  `__len__` (local scan + index), `__contains__`, `get`/`__getitem__`, `get_timestamp`,
  `get_items`, `load_items`+`items`/`values`/`timestamps`/`map` — all resolve through the
  local file and/or index, both cleaned. `RemoteConnGroup` overrides nothing relevant.
- **Remote-gone / uuid-change safety**: a deleted remote never reconciles (fetch gated on
  `initialized`); a re-created remote with a new uuid raises `UUIDMismatchError` before
  ingest — no mass-deletion path through either.
- **Equality boundary** `ts <= prev_synced_ts`: safe — materialized values satisfy
  `entry_ts < commit_ts` on the pusher's clock (changelog build precedes phase C; the
  skew-bump `max(now, remote+1)` at utils.py:877 still precedes the commit's
  `make_timestamp_int`), except under user-supplied stamps (MAJOR-1).
- **Logging plan and the `fetched` signature change**: fine as designed.

**What I did not verify:** no runtime experiments (read-only session) — the defect,
resurrection hazard, and pulled-value timestamp semantics are established by direct code
trace only; cfdb/EDataset behavior (outside my permitted read scope) — in particular
whether any stack consumer passes explicit timestamps, which is the deciding input for
MAJOR-1's real-world severity.
