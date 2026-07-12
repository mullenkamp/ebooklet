# ebooklet Phase 1 design plan — independent review (Claude, fresh context)

Reviewed against working trees: ebooklet 0.9.5, booklet 0.12.6, s3func 0.9.2 (installed venv
copies verified byte-identical to the working trees for booklet and s3func; ebooklet editable).
All experiments run locally: booklet directly, ebooklet through `tests/fake_s3.py` driving the
real remote/push code. No repo files were modified; throwaway scripts live in /tmp
(`exp1_booklet_meta.py`, `exp2b_ts.py`, `exp3_msgspec.py`, `exp4b_min.py`, `exp4c_restart.py`,
`exp5_delete_set.py`).

## Verdict

**Implementable with changes.** The architecture (manifest-map, generational immutable groups,
journal in a reserved slot, upload-then-commit-then-sweep) is sound and the plan's line
references are almost uniformly accurate. But the journal's delete semantics have a hole that
today already loses data (F-1, proven empirically on 0.9.5) and that the plan as written would
make persistent; the replacement-push sweep needs a second `lock.verify()`; the
'w'-over-v1 recovery exception is under-specified and can ingest a v1 index body; the metadata
carry-forward rule mishandles both replacement pushes and skewed local metadata; and the
prune-exclusion of journaled keys has no mechanism in any stage. Fix those in the plan (all are
plan-text-level changes, none invalidates the architecture) and this is ready to implement.

---

## Findings

### F-1. CRITICAL — delete-then-set of the same key loses the key on push; the journal spec does not fix it and would make it persistent

**Evidence:** ebooklet/main.py:753-762 (`__delitem__` adds to `_deletes`, removes from
`_written_keys`), main.py:439-448 (`set` adds to `_written_keys` but **never removes from
`_deletes`**), utils.py:712-717 (upload loop writes the fresh index entry), utils.py:729-734
(deletes loop then removes it: `if key in remote_index: del remote_index[key]`).

**Empirical proof (0.9.5, /tmp/exp5_delete_set.py):** one session: `del eb['key2']` then
`eb['key2'] = b'NEWVAL'` then push → returns True; fresh reader: `'key2' in r` is **False**,
`r['key2']` raises KeyError. The group object contains the new bytes; the pushed index does
not reference them. Silent data loss in the currently released version.

**Why the plan makes it worse:** `journal.deletes` is persistent, "Deletes come only from the
journal", and replay-on-swap re-applies `journal.deletes` against every fresh index. A key
deleted-then-rewritten would be simultaneously in `journal.written` and `journal.deletes`
forever (deletes cleared only per successful push-group); replay-on-swap would keep deleting
its index entry cross-session.

**Required plan change:** state the invariant `journal.written ∩ journal.deletes = ∅`:
`set`/`set_timestamp` must remove the key from `journal.deletes` (dual of the existing
`_written_keys.discard(key)` in `__delitem__`, main.py:762), and add a pre-fix proof test for
delete-then-set-then-push to the Stage-2 test list. (Worth a 0.9.x hotfix consideration too,
since this is live today, but that is outside this phase's scope.)

### F-2. MAJOR — replacement-push sweep must re-verify the lock ticket immediately before sweeping

**Evidence:** decision 7 / Stage 3 ("Replacement … same A–C, then sweep all of `db_key + '/'`
not in the new manifest"); Stage 1 puts `lock.verify()` at push start and before the phase-C
commit PUT only.

**Failure scenario:** replacement writer W1 commits C, then stalls before the sweep. Its
ticket is broken (age-gated force_lock from W2 — W1's session has legitimately been open a
long time; the age gate keys off ticket upload time, i.e. session start). W2 acquires, pulls
W1's committed v2 state, starts its own push phase B, PUTs fresh gens. W1 wakes and sweeps
"everything under `db_key + '/'` not in *W1's* new manifest" — which includes W2's
uncommitted fresh gens. W2 then commits a manifest referencing deleted objects → permanent
RemoteIntegrityError for every reader. Note the normal-push phase D does *not* have this
problem (it deletes only exact old-gen keys from W1's own pre-push snapshot, which no
successor can be referencing as *new* data) — the prefix-complement sweep is the outlier.
One more `lock.verify()` immediately before the sweep (and abort the sweep, log-only, on
failure — the commit already succeeded, orphans are fsck's job) closes all but a
milliseconds-wide window, consistent with the accepted no-CAS lock model.

### F-3. MAJOR — the 'w'-over-v1 `replace_pending` recovery exception lacks the v1 carve-outs and can ingest a v1 index body into the sidecar

**Evidence:** Stage 3 upgrade mechanics grants the four carve-outs to `flag='n'` only; the
'w' refusal exception says only "'w' over a v1 remote is allowed iff the local journal
carries `replace_pending=True`". But the 'w' open path runs `check_local_remote_sync`
(utils.py:153-170) → `overwrite_remote_index=True` whenever `remote_session.timestamp >
local_file._file_timestamp` → `get_remote_index_file`/`fetch_remote_index` (utils.py:211-240)
downloads the **v1 db body** into the sidecar. The crashed-'n' local file was created from the
v1 remote's `init_bytes`, so the uuid check passes; if any other writer bumped the v1 remote
after the crash (the crashed session's ticket is exactly what the age-gated sweep removes),
the timestamps differ and the fetch fires. The session would then hold a v1 index (old
`str(gid)` offsets, no manifest) inside a v2-era session — grouped reads through it are
garbage, and `_pull_remote_index` (main.py:600-649) has the same hole mid-session.

**Required plan change:** the 'w'+`replace_pending` recovery mode must apply carve-outs
(i)/(iv) as well — suppress all v1 index-body fetches (open-time and `_pull_remote_index`)
until the v2 commit; its reads serve only the local replacement image. Cleanest statement: a
session opened over a format-1 remote in ANY allowed mode ('n', or 'w'-recovery) is
"index-fetch-suppressed" until its own v2 commit.

### F-4. MAJOR — metadata carry-forward: wrong for replacement pushes, and loses skewed local metadata

Two sub-defects in decision 9 ("embed local metadata iff local slot ts ≥ cached remote meta
ts (slot 2), else carry the cached remote `{ts,data}` forward"):

(a) **Replacement pushes must not carry forward.** Today `flag='n'` + push wipes `_metadata`
(utils.py:577-579 → `delete_remote`); the new database has no metadata unless this session
set it. Under the plan's rule, a replacement push from a session that never called
`set_metadata` (fresh local slot ts None/0 < cached remote ts) would carry the OLD remote's
metadata into the replacement — resurrecting exactly what 'n' semantics promise to destroy.
Rule: when `replace_pending`, embed the local slot or nothing; never the carried remote copy.

(b) **`meta_pending` must override the timestamp comparison.** A local `set_metadata` from a
clock-skewed machine gets slot ts < cached remote ts → the embed condition fails → the
user's explicit, unpushed metadata is silently replaced by the remote copy on push — the
exact 0.8.4 skew bug the plan closes for keys via skew normalization, reintroduced for
metadata. Rule: `meta_pending=True` → embed local (normalizing its ts to
`max(now, remote_meta_ts+1)`, mirroring the key rule); the ts comparison is only the
tiebreak when `meta_pending=False`. As specified, the `meta_pending` flag exists in the
journal but decision 9's embed rule never consults it.

### F-5. MAJOR — `prune()` exclusion of journaled keys has no mechanism in any stage

**Evidence:** Stage 2 "Guards & edges": "`prune()` must not evict journaled-pending keys
(exclude journal.written from timestamp eviction)". But ebooklet.prune (main.py:492-516) just
calls `self._local_file.prune(timestamp=...)`, and booklet's `prune_file`
(booklet/utils.py:1035-1177) has **no exclusion parameter** — its keep/evict decision at
1087-1094 knows only the metadata hash and the timestamp. Stage 0's booklet 0.12.7 scope
(reserved-slot registry only) does not add one. As written, Stage 2 hits an implementation
wall: either booklet grows `prune(protected_hashes=frozenset)` (belongs in the 0.12.7 stage,
+0.5d, needs the same fixed-variant NotImplementedError treatment), or ebooklet refuses/warns
on `prune(timestamp=...)` while `journal.written` is non-empty, or re-materializes journaled
keys post-prune. Pick one and put it in the plan; the loud-warning drop in the changelog
union is a symptom detector, not a fix.

### F-6. MAJOR — crash-window deletes are silently lost; the union changelog does not cover them

**Evidence:** Stage 2 persists the journal at sync boundaries, "NOT per-write (write
amplification; the sync-window residual is the accepted union-changelog gap)". But the union
covers only *writes* (data blocks durable + ts diff); "Deletes come only from the journal".
`__delitem__` applies the delete to the local file and sidecar **immediately** (booklet
main.py:596-619 writes delete flags unbuffered), so after a hard crash (finalizer does not
run on SIGKILL/power loss) the local state shows the key deleted while the journal has no
record — the next `_pull_remote_index` resurrects it locally and the remote never learns of
the delete. That is Seam 2's headline bug, narrowed to a window but in its *silent* form.
Deletes are rare relative to writes and booklet's delete is already an immediate write: the
write-amplification argument does not apply to them. **Persist the journal in `__delitem__`**
(deletes only; writes keep sync-boundary persistence), or explicitly document the window as
accepted — the plan currently implies the union covers the whole crash window, which is
false for deletes.

### F-7. MAJOR — journal-persist-at-sync() creates new "booklet mutated during iteration" failures in existing consumer patterns

**Evidence:** booklet sync() (main.py:700-715) does not bump `_mutation_count`; a journal
persist via the set_metadata-style reserved write DOES (main.py:96 pattern), invalidating
every open iterator on the local file (`_iter_locked`, main.py:194-206). Persist points
include `sync()`, and ebooklet.sync() is called by `Change.__init__` (main.py:70),
`_pull_remote_index` (main.py:607), and consumers directly (cfdb edataset.py:35 syncs before
every `changes()`; it documents mid-session push as safe). Pattern that works today and
breaks under the plan: iterate `eb.items()` (a local_file iteration) in one thread while
another calls `sync()`/`changes()`/`pull()` — today the flush doesn't invalidate; with an
unconditional journal persist it raises RuntimeError. Fix: **persist only when the journal is
dirty** (writes that dirty it already invalidated iterators themselves, so the persist rides
existing invalidation), and note the residual in the changelog. Empirically confirmed the
underlying booklet behaviors: `set_timestamp` during iteration allowed; `set` raises
(/tmp/exp2b_ts.py).

### F-8. MAJOR — `journal.num_groups=None` cannot distinguish "per-key chosen" from "not recorded"; the 0.9.1 reopen warning survives for per-key files

**Evidence:** Stage 2 "num_groups rides the journal … kills the 0.9.1 reopen warning
(main.py:291-299) for journaled files"; journal schema `num_groups:int|None`. A database
deliberately created per-key (`num_groups=None`) journals None — indistinguishable from a
pre-journal file or an unset value, so the reopen path still can't tell "per-key was chosen"
from "the user forgot", and either the warning stays (noise the plan claims to kill) or it is
dropped (recreating the original silent-per-key-push hazard for any file whose journal is
missing). Use a tri-state: e.g. `num_groups: int | None` plus `num_groups_set: bool`, or the
sentinel 0 = per-key-chosen, None = unrecorded. Cheap now, breaking later (the blob outlives
0.10.0 per the `v` field's own rationale).

### F-9. MINOR — `_metadata` kill-list omissions

The Stage-3 kill list (utils.py:502-527/565/591-593/607/623-624/691/704; main.py:372/559-561/
696-702/722; remote.py:249-262) omits:
- **utils.py:299 and 305-306** — `get_remote_value`'s `s3_key = '_metadata'` mapping and its
  `set_metadata(orjson.loads(...))` branch (the function survives for per-key values, so the
  metadata branch must be excised explicitly, and it is one of the four orjson sites);
- **main.py:786-794** — the close() warning "deletions are tracked in memory only and are
  LOST when the session closes" becomes false once the journal lands and must be removed;
- **main.py:149/153** — `metadata_key_str` exclusions inside the flag-'n' purge (the block is
  rewritten in Seam-1 anyway; listing it prevents a stale copy);
- **main.py:359** — `get_metadata`'s `_load_item(metadata_key_str)` becomes a structural
  no-op in v2 (no index entry); fine, but say so, or remove the call.

### F-10. MINOR — crash-before-C leaves the local sidecar referencing new gens under the old manifest (self-healing but expensive; consider staging)

Phase B updates local index entries per successful group ("composes with existing
bookkeeping", i.e. utils.py:712-718 semantics) before C builds the payload. Crash before C:
local sidecar has new-gen offsets, slot-2 manifest still has old gens → reads of unwritten
keys in those groups do ranged GETs into the OLD object at NEW offsets → header verification
fails (utils.py:417-435) → full-object `recover_group_members` download per read. Correct but
costly, and it silently exercises the recovery path. Alternative: stage index-entry updates
in memory and apply them when building the C payload (failures dict already carries per-group
state). At minimum, document that the header-verification fallback is load-bearing for this
crash state.

### F-11. MINOR — spec gaps worth one line each

- **`clear()` vs the journal:** "after `clear()`" is a persist point, but the plan never says
  what clear() does to the sets. Local values are gone, so `journal.written` entries become
  the loud-warning drop case at the next push; `journal.deletes` should survive. State it
  (written cleared, deletes kept, replace_pending kept).
- **Finalizer re-registration:** `_pull_remote_index` re-registers the finalizer with new
  args (main.py:643-644); the JournalState must be threaded through there too, and the
  persist must happen **before** `local_file.close()` inside `ebooklet_finalizer`
  (utils.py:128-136).
- **Slot-2 persist ordering:** persist slot 2 before `_set_file_timestamp` in
  `_pull_remote_index` (main.py:649), else a crash between them makes the stamp claim
  freshness over a stale manifest (self-heals through F-10's path, but ordering is free).
- **Initial-open fetch parity:** the payload parse (manifest→slot 2, metadata refresh) must
  run on the `get_remote_index_file`/`fetch_remote_index` open path (utils.py:211-240), not
  only inside `_pull_remote_index` — cfdb's create flow calls `get_metadata()` immediately
  after open on a fresh local file (cfdb/edataset.py:131-133) and relies on it.
- **`gen13` re-roll belt:** on generating a new gen for a group, re-roll if it equals the
  current manifest gen (52-bit collision is ~2⁻⁵² per push, but the consequence is exactly
  the in-place-overwrite bug being eliminated; the guard is one line).
- **FakeLock needs `verify()`** (tests/fake_s3.py:44-60) before Stage-2 hermetic tests can
  drive push.
- **`_resolve_missing` local-cleanup belt:** exclude `journal.written` from the
  `del self._local_file[k]` cleanup (main.py:702) — unreachable if the gate is complete, but
  it is the one place a stale marker could delete an unpushed local value.
- **Line-ref drift (trivial):** `fetch_remote_index` 200-check is utils.py:233 (plan says
  236); booklet fixed-prune compensation is utils.py:2098 (plan says 2099).

### F-12. MINOR — reserved slots remain hash-reachable through ebooklet's `get`/`__contains__`

Stage 2 guards `set`/`set_timestamp`/`__delitem__` against reserved key strings, but
`key in eb` (main.py:464-468) and `eb.get(journal_key_str)` would return True/the raw blob
(same asymmetry the metadata key has today). Either add the same guard to the read paths or
document the asymmetry as inherited.

---

## Answers to the numbered questions

1. **booklet reserved slots — complete?** Yes. Verified all key-enumeration paths: file iter
   (utils.py:554), mmap iter (utils.py:730), map scan (main.py:803) are the only
   variable-length yield sites; fixed-length iterators have no guard because fixed booklets
   cannot hold metadata/reserved keys (NotImplementedError, main.py:1015-1026 — mirror it).
   The crash-recovery `_n_keys` rebuild goes through `self.keys()` (utils.py:1302-1308), so
   it inherits the skip (verified empirically: rebuild counted 3 user keys, excluded
   metadata). `prune_file` skips by hash (1088) with count compensation (1174); the plan's
   counter generalization is right. `len()` uses `_n_keys`; the set_metadata write pattern
   (pre-sync, then discard `n_extra_keys`, main.py:94-100) keeps reserved writes out of the
   count — but note `sync()`/`_sync_index` (main.py:717-719) would count a reserved key if
   one ever sat in the shared buffer, which the pre-sync prevents; keep that pre-sync
   mandatory in `set_reserved`. The count-skew delete bug is real (verified: deleting the
   metadata key drops len 2→1 wrongly; main.py:613-615). `reindex` moves blocks by hash
   without yielding keys — no leak. Bytes-in/bytes-out is the right contract (booklet stays
   serialization-agnostic; the msgspec boundary stays in ebooklet). Two-slot registry: sound;
   consider defining slots as a small table (bytes, hash, name) so a third slot is a one-line
   addition.
2. **Journal persistence/crash windows:** finalizer persistence is safe only if it runs
   before `local_file.close()` in `ebooklet_finalizer` and is threaded through the
   `_pull_remote_index` re-registration (F-11). Persist-at-sync creates new
   iterator-invalidation failures unless persist-if-dirty (F-7). Crash-window WRITES are
   covered by the union (booklet auto-flush at write_data_blocks:911-914 makes data durable;
   ts diff catches it). Crash-window DELETES are **not** covered — silently lost (F-6).
3. **Per-group clearing:** the safe rule — clear `journal.written`/`journal.deletes` entries
   only **after the phase-C commit PUT returns 2xx**, for exactly the groups (or per-key
   uploads) whose new state that commit references; replacement pushes clear only on a full
   replacement commit (which requires zero phase-B failures, since partial replacements
   commit nothing). Never clear on C failure or pre-C crash. Stage 3's C clause has this
   right ("On success: clear journal per-group"); Stage 2's "exact dual of retained_deletes
   (utils.py:727-736)" is misleading — that code runs *before* the db-object PUT today, and
   copying its position would let a C failure (or the 'n' purge on retry) eat locally-held
   keys. State the rule once, in the C clause, and make Stage 2 refer to it. A crash between
   C success and journal clearing merely re-pushes those groups (harmless orphan gens).
4. **Read-your-writes gate completeness:** the three sites + pull-loop cover it, with
   provisos: the pull-loop's `length==0` branch materializes `b''` at utils.py:647 — it is
   inside the same `check` gate at 630, so gating the condition covers it (make the
   implementation gate the *condition*, not the download list). `recover_group_members` and
   `get_remote_group_values` only materialize keys in their `key_infos`, which gated keys
   never enter. `map()` funnels through `load_items`. `_resolve_missing`'s purge path cannot
   see journaled keys if the gate is complete, but add the belt (F-11). Metadata refresh is
   the remaining clobber path and is governed by `meta_pending` (F-4b must be fixed for it to
   actually protect).
5. **Replay-on-swap:** yes — `open_remote_index` is called exactly twice (main.py:279 initial,
   main.py:640 pull). Writable sidecars in 'r' sessions are concurrency-safe: the local file
   is opened 'w' with a **blocking exclusive portalocker lock** in every ebooklet mode
   (utils.py:191; booklet utils.py:1265/1362 `LOCK_EX`) — verified empirically (a second
   same-file open blocks indefinitely), so a second process never reaches the sidecar.
   Side-effects to note: 'w'-mode sidecars lose the mmap read path (minor perf) and become
   crash-rebuildable (the n_keys_crash rebuild needs write access — now available). Replay
   must tolerate absent keys (`if k in idx`).
6. **Skew normalization:** safe and correctly ordered. `set_timestamp` during iteration is
   explicitly allowed (verified empirically); the pull-loop iterates `remote_index` while
   normalizing `local_file` — different booklets, no interaction. Ordering: it need not
   precede `create_changelog` — packing (utils.py:322) and index-entry writes (utils.py:714)
   re-read local timestamps, so normalizing in the pull-loop (phase A) is sufficient; the
   changelog file's stored ts is display/membership only. The union guarantees membership
   regardless. Document that the changelog's displayed ts may predate normalization, or
   normalize before `update()` for cosmetic consistency.
7. **Manifest-map vs per-entry generation:** pressure-tested; manifest-map holds. The only
   scenario where per-entry gen could win is index-entries-newer-than-manifest desync, which
   cannot occur when both ride one PUT and the reader swaps them atomically under
   `_index_lock` (the plan specifies this). Recovery (`recover_group_members`) is gen-agnostic
   once the fetch is manifest-routed; copy/fsck actively *prefer* the manifest. Per-entry gen
   adds 7 bytes × keys, a new drift bug class, and an index-format migration for zero
   additional invariants. Concur with the plan, including the rejection reasoning.
8. **Payload layout:** sound. Magic discrimination verified: v1 bodies are raw booklet
   FixedLengthValue files beginning with `uuid_fixed_blt` (booklet utils.py:54; ebooklet
   asserts the analogous uuid prefix at utils.py:774), which cannot collide with
   `b'ebooklet-db\x00'`. Ranged GETs work on both readers (S3 path and
   HttpSession.get_object, s3func/http_url.py:109-136, builds Range headers); the fake
   returns 206/416 faithfully. `>Q` is right (index sections can exceed 4 GiB in principle;
   16 wasted bytes are nothing). Keep HEAD-driven fields (uuid/ts/type/init_bytes/num_groups/
   format_version) in S3 metadata — all pre-body needs, verified against `_load_db_metadata`
   remote.py:181-211. Embedding metadata+ts kills every listed path **plus** the omissions in
   F-9. Two ranged GETs (header, then manifest+meta) per refresh — or one speculative range —
   implementation's choice.
9. **Push protocol crash states:** enumerated and consistent. B partial failure → C commits
   successful groups; failed groups keep old gen/old entries/retained deletes+written;
   abandoned gens invisible. Crash before C → nothing committed; local sidecar mixed state
   self-heals via header verification (F-10). C PUT failure → raise; retry force_push repacks
   to fresh gens; previous gens orphan. Crash between C and D → readers fully consistent on
   the new manifest; old gens orphan; fsck sweeps. D failure → log-only, verified "nothing
   depends on it": index+manifest committed, copy_remote manifest-driven, delete_remote's
   bounded `db_key + '/'` prefix catches them, fsck sweeps; old-manifest readers transiently
   *benefit*. Emptied groups: verified against today's code — `upload_group`'s empty-entries
   branch deletes the object pre-commit (utils.py:335-341, the delete at 338); the new
   no-PUT/drop-from-manifest/GC-at-D lifecycle removes the last pre-commit remote mutation.
   The one hole is the replacement sweep (F-2).
10. **`_resolve_missing` retry:** one-shot is correct and sufficient in expectation; a false
    RemoteIntegrityError needs two writer commits + GCs interleaving one reader operation —
    acceptable, but say in the docstring that the error can (rarely) mean "extremely hot
    writer", not corruption. The uuid-None refinement composes: it is a pre-check on the same
    re-pull result, and it should reuse the legit-deletion local-cleanup path (delete
    materialized values / null metadata) so a torn-down remote doesn't leave stale local
    reads.
11. **Metadata carry-forward:** broken in two ways as specified — replacement resurrection
    and skewed-local loss (F-4). With those fixed: fresh-local-first-push preserves remote
    metadata (verified the premise: fresh local files always fetch the db body at open, so
    slot 2 is populated); `meta_pending` protects unpushed local metadata from pull overwrite
    (plan already gates the `_pull_remote_index` refresh on it); RCG.add's member snapshot via
    the ranged parse works (member remotes are read with fresh sessions in `rc.open()`,
    remote.py get_user_metadata → new parse).
12. **flag='n' over v1:** the four carve-outs cover the 'n' path itself (traced
    utils.py:139-208 + main.py:213-341: the only other remote touches are HEAD-driven
    metadata, the writable probe, and the lock — all fine). The gap is the **'w' recovery
    exception**, which needs the same suppression (F-3). Also specify: 'n' over v1 with
    `num_groups` omitted must not inherit v1 `num_groups` (carve-out ii) — but then a
    replacement that *wants* the same grouping must re-pass it; say so in the upgrade recipe.
13. **F7 guard:** placement and shape are right; verified the worker contract
    (`upload_group` returns errors; a raise would crash `future.result()` — the invariant is
    documented at remote.py:364-372). The single bound is sufficient for both fields: if
    final `pos` ≤ 2³²−1 then every offset and every `len(value)` fit 4 bytes
    (`int_to_bytes(offset,4)` at utils.py:717 and `struct.pack('>I', len(value))` at
    utils.py:89 are the two overflow sites today). Check `pos + entry_size` *before*
    appending to avoid materializing the oversized buffer — the plan says exactly this.
14. **fsck:** expected set is right — db object at `db_key` sits outside the `'/'` listing;
    lock tickets live at `db_key + '.lock.'` (locking.py:166), outside the prefix; aged
    probes sweepable. Under the write lock the db object cannot change mid-fsck, so
    uuid/timestamp oddity checks add little; but fsck SHOULD refuse (or warn) if the db
    object is absent while children exist (torn delete_remote), and should treat
    `format_version` != 2 as a hard stop. The 24h age gate + write lock is adequate for the
    crashed-commit-retry scenario given the documented "retry promptly or discard" contract.
15. **s3func lock changes:** 1h default break age is defensible **only because** `verify()`
    makes the victim fail safe (pre-commit abort; retained journal; retryable) — bulk pushes
    in this stack can exceed 1h, and the ticket timestamp reflects session start, not
    activity. Document the victim's outcome in the force_lock docstring, and consider 2-4h.
    Own-ticket exclusion is sound: a crashed-and-reopened process has a new
    `uuid4().hex[:13]` lock_id (locking.py:202), so its stale ticket is breakable as
    "other"; the only unbreakable tickets are the live instance's own, which is correct
    (the current default deletes the caller's own fresh ticket — verified the
    `upload_timestamp <= now` path at locking.py:391-409). `verify()` under listing
    staleness: the self-visibility gate (locking.py:304-316) already establishes
    the-listing-shows-own-tickets as the trust precondition; post-acquire, a listing that
    drops a long-since-PUT object is a deletion signal on S3/B2 in practice, and the failure
    direction (spurious abort) is safe. Concur.
16. **msgspec:** verified (msgspec 0.21.1): int-keyed dicts encode to string JSON keys and
    round-trip **only** with `type=dict[int,str]` — specify the typed decode (or store gids
    as strings end-to-end). Struct decode ignores unknown fields and defaults missing ones by
    default (verified) — right posture for a slot that outlives 0.10.0: keep non-strict, gate
    on `v` (`v` > supported → refuse loudly, don't default-parse). Don't set
    `forbid_unknown_fields`.
17. **Consumers:** contracts hold. cfdb relies on push() returning True/False/dict
    (edataset.py:44-51) and envlib's `_raise_on_push_failure` (catalogue.py:60-67) on the
    dict-means-partial convention — the F7 failure entry as a dict value is
    isinstance-compatible. cfdb's create flow needs F-11's initial-open parse parity
    (get_metadata right after open on a fresh local file, edataset.py:131-133). cfdb's
    mid-session push safety note (edataset.py:46) depends on the 'n'→'w' downgrade
    equivalent — `replace_pending` cleared on successful replacement push preserves it.
    cfdb calls `sync()` before every `changes()` — F-7's persist-if-dirty matters here.
    envlib's publish/deregister flows are RCG + user_meta round-trips — unaffected if entry
    schema v1 stays frozen (it does; only the *member-metadata snapshot's* source changes).
18. **The three residuals:** (1) lock orphans — closed *when force_lock is used*; the
    age-gate makes force_lock safe to use routinely, which is the operative change; nothing
    sweeps automatically (fine, document). (2) whole-remote-absence — closed by the uuid-None
    branch, provided it shares the local-cleanup path (Q10). (3) copy_remote orphan
    replication — closed; parsing the per-key index via FixedLengthValue over BytesIO is
    feasible (verified: read-mode BytesIO open works), and gen tokens copy verbatim.

---

## Examined and held (load-bearing claims verified)

- **Line references:** essentially all checked refs are accurate against the working trees —
  booklet utils.py:56-58/554/730/1088/1174, main.py:81-133/613-615/803, n_keys crash rebuild
  via keys(); ebooklet main.py:146-155/174-175/182/219-241/265-270/291-299/331/341/557/575/
  636-644/719/753-762/920-921/927, utils.py:77-94/128-136/139-150/145/167-168/229/236(→233)/
  256-259/319-341(338)/478-529/577-579/630-632/727-736/764/774, remote.py:145/181-211/249-262;
  s3func locking.py:136-138/202/302-316/391-409. Drift found: two trivial (F-11).
- **Stage 0 premises (empirical):** metadata invisible to keys()/len; repeated set_metadata
  stable; prune preserves metadata + count; **the count-skew delete bug is real** (len 2→1
  after deleting the metadata key); crash-recovery n_keys rebuild excludes metadata.
- **Decision-4 premise (empirical):** a restarted reader session on an in-sync local file
  performs **zero** GETs at open and serves materialized reads with no remote fetch — a
  memory-only manifest would indeed strand grouped reads; slot 2 is necessary.
- **Storage layout (empirical):** grouped push produces `db_key/{gid}` objects + `db_key` db
  object; unmaterialized reads do one ranged group GET; num_groups arrives via HEAD.
- **booklet local-file exclusivity:** 'w' opens take a blocking `portalocker.LOCK_EX`
  (source + observed blocking), so writable sidecars in 'r' sessions add no cross-process
  hazard.
- **set_timestamp during iteration allowed; set() raises** (empirical) — the skew
  normalization's mutation is legal where planned.
- **msgspec behaviors** (empirical): typed int-key decode; unknown-field tolerance;
  missing-field defaults.
- **v1/v2 discrimination:** booklet files start with the 16-byte type uuid; the 12-byte magic
  cannot collide.
- **Worker return-not-raise contract** documented and load-bearing at remote.py:364-372.
- **0.9.5 format_version guard** exists at the single reader choke point (remote.py:189-197),
  so "old clients refuse new-format remotes loudly" is real, including for db_url readers.
- **fake_s3 fidelity** for the hermetic tier: 206/416 ranged GETs, prefix deletes, metadata
  round-trip — adequate for Stages 2-4 tests except FakeLock.verify() (F-11).

## Verification note

**Checked:** everything above by direct source read of the four repos + five /tmp experiments
executed against the installed (working-tree-identical) packages; git status of all five
repos audited before and after — no repo files created, modified, or deleted by this review.
**Not verified:** live-S3 behaviors (listing consistency, version purges — the fake's
documented fidelity limits), booklet fixed-length BytesIO in 'w' mode (only 'r' tested;
copy_remote needs only 'r'), and no Stage-0/1 code was prototyped (design-level review only).
