<!-- ai-review-harness
round:      ebooklet-wog-code-1
agent:      openrouter
requested:  model=z-ai/glm-5.3:exacto effort=high
resolved:   model=z-ai/glm-5.3:exacto
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-code-1.md
scopes:     ebooklet cfdb envlib envlib-ingest-base
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-07T09:38:20+13:00
-->

<!-- staged cfdb: 1736 KiB of 5488 KiB (3752 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8724 KiB of 230268 KiB (221544 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1464 KiB (708 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-base: 4368 KiB of 369548 KiB (365180 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 37 agent turn(s); the report is whichever is substantive -->
I'll start by orienting myself in the `ebooklet` scope: the diff, the plan, and the layout.
Now let me read the core diff. It's large, so I'll take it file by file.
Now I have the full picture of the diff. Let me set up the environment and read the key files in context.
203 passed, matching the claim. Now let me read the full current source of the push engine and open path in context.
Now let me read the current `main.py` open path and pull path in full context.
Let me look at the push pre-check and `_pull_remote_index` handle swap.
Now the decisive cross-version experiment the author flagged as not yet run: a 0.11 per-key remote opened by 0.10.5, and the reverse.
Cross-version per-key interop verified both ways. Now the journal v1 interop — 0.10.5 opening a local file whose journal 0.11 persisted:
Let me check the downstream scopes now — ingest-base's diff and the new cfdb test file — and the s3func listing field fsck relies on.
s3func's listings do carry `content_length`, so fsck's `dead_fraction` works against the real transport. Now the highest-value experiment: a randomized multi-writer stress test driving the real `open_ebooklet`/push/read paths, checking committed-state invariants after every push.
A failure at iteration 8. Before claiming anything, let me instrument the run to see the exact operation sequence.
stdout/stderr interleaving is unreliable here. Let me rerun with the trace on stderr and more detail.
The sequence is now clear. Let me dump the actual state at the failing iteration to see what the push did.
That failure is my fuzzer's model bug — the 10% no-push branch still checked remote state. Let me make the push unconditional and rerun several seeds.
That orphan is the documented crash-window residue (commit landed, phase-D GC skipped). Expected behavior — my invariant was too strict. Let me relax it for crash iterations and keep fuzzing.
My sed-editing made a mess. Let me look at the file and fix it properly.
Orphans are appearing on ordinary paths, not just crash windows. Let me instrument seed 2 iteration 0 precisely.
The orphan is from a same-session retry after a crash-after-commit: the retry can't GC generations its stale cache never knew — benign, invisible orphans. Let me make the flag sticky and rerun.
All 8 seeds pass. Now the remaining targeted suspicions. First, the migration script against the fixture (Q7):
The migration script builds a real `S3Connection` from a toml; I'll exercise it against the fixture by patching the connection factory, under 0.10.5 as designed.
The `objects left: 5` in my check is just the untouched on-disk fixture — the in-memory deletion was clean (`gone=True, objects left: 0`). Now the refusal conditions:
The fixture itself has no group-2 object (empty under the hash) — my mutation deleted nothing. Let me redo it against a group that exists:
All refusal conditions verified. Now let me examine `__delitem__`/`__setitem__` for a suspicion about the staged-delete removal being dead code, and check the remaining test-mutation claims.
`__delitem__` removes the sidecar entry immediately, and open-time replay applies deletes to the sidecar too — so the staged-copy `committed_delete_keys` removal looks like pure belt. Before ranking that as a simplicity finding, one reachability check in `utils.py`:
`committed_delete_keys` only matters if a deleted key's entry can still be present in the sidecar at push time. `__delitem__` removes it immediately, but there's one path I haven't verified: a sidecar **re-fetched from the remote** (layout mismatch or lagging remote state) — does open-time journal replay strip journaled deletes from the freshly fetched index? If not, the staged copy would carry the deleted key and the belt at 1691 would be load-bearing.
The replay runs both at open and after every pull handle-swap, so the sidecar never holds a journaled-deleted key at push time — the `committed_delete_keys` staged removal is confirmed belt-only. Now the mutation spot-checks. First: is the "per-key journal stays v1" guarantee pinned by any suite test?
`test_journal_version_follows_the_mode` pins the v1/v2 split. Let me read it, then run the mutation spot-checks.
Now the mutation spot-checks. **Mutant 1**: journal always stamps v2 (breaks the "per-key stays v1" compat guarantee).
Mutant 1 killed. **Mutant 2**: remove the open-time replay of journaled deletes onto the freshly fetched index (main.py:633-635).
**Mutant 2 SURVIVED** — the open-path delete-replay was removed and all 89 tests still pass. Let me confirm the guard is actually load-bearing (observable resurrection) with a direct scenario, so this is a genuine test gap rather than dead code.
Confirmed: with the replay removed, the deleted key **resurrects** through `__contains__`/`keys()` after a re-fetch — but no test catches it. Let me be fair and run the full fake-S3 battery against both replay-site mutants before calling it a gap.
The full battery kills it — `test_stale_incarnation.py::test_deletion_intent_survives_a_misfire` catches the open-path replay removal. My earlier 4-file run was incomplete (I guessed wrong file names). Now the **second** replay site, inside `_pull_remote_index`:
Pull-path replay covered by `test_replay_on_swap_prevents_resurrection`. Both replay sites are properly pinned. Last spot-check: the sidecar-layout rule — both directions.
Direction (a) killed; direction (b) — never discard online — **survives** both suites. Let me check whether that online branch is reachable at all, and also verify the author's claimed survivor (copy_remote branching on manifest vs storage kind):
Mutant 5 survives — the author's survivor claim reproduces. Let me read why `test_copy_remote_of_an_emptied_grouped_remote` doesn't kill it:
The mutant is semantically equivalent on all reachable states (per-key remotes always have an empty manifest; an emptied grouped remote has an empty index too), which explains the author's survivor. Now verify all mutations are restored and the tree is green:
All evidence is gathered. Here is the final deliverable.

---

# Review verdict: ebooklet 0.11 write-order groups (format 3)

**Bottom line: I found no data loss and no corruption on any path I could execute.** The core claims held up under adversarial testing. The findings below are nuances, test-gap observations, and simplicity notes — none is a "wrong, loses/corrupts data" defect.

## Ranked findings

### 1. [accuracy — verified, low severity] Same-session retry after a crash-after-commit bypasses the StaleIndexError belt and orphans the crashed commit's generations

After a push whose commit PUT succeeded but which died before `_note_commit` ran, a **fresh open** heals correctly (the `remote_state.remote_ts != remote_session.timestamp` re-fetch at `main.py:594-598` fires). But a retry **in the same session** does not: `remote_state.remote_ts` and `remote_session.timestamp` are both stale, the belt's precondition is false, and the retry re-commits from the stale cached manifest — so the GC misses the crashed commit's group generations and leaves them as orphans.

- **(a)** Executed: my 2-writer fuzzer initially failed its orphans assertion on seeds `33.c0ce...` and `0.74def...` exactly at crash-after-commit → retry sequences; I traced the cause to `pre_push_manifest` never learning the crashed commit's generations, fixed the fuzzer by making allow-orphans sticky after a crash op, and all 8 seeds × 200 iterations then passed. Derived from source: `_note_commit` (`remote.py:375-380`) is the only thing that advances `remote_session.timestamp`, and it runs after the commit PUT.
- **(b)** Not checked: whether the orphaned generations are ever reader-visible (analysis says no — readers pair index+manifest from one commit; the fuzz fresh-reader value verification never saw stale data). Benign by design: invisible orphans, fsck-sweepable. This is consistent with the documented crash-window story, but the `StaleIndexError` docstring's "should be unreachable" is slightly stronger than reality — the belt catches the cross-session case, not the same-session one.

### 2. [accuracy — verified, minor] Migration script exits with a raw `UnsupportedFormatError` traceback on a format-3 remote

Against a format-3 remote, `hydrate_and_delete.py` raises an unhandled `UnsupportedFormatError` (raw traceback) instead of the clean `ABORT: ... Nothing deleted.` message every other refusal uses. Safe — nothing is deleted — but inconsistent UX for the one input a confused operator is most likely to feed it.

- **(a)** Executed under real 0.10.5 with `FakeS3Connection`: `/scratch/mig/scenarios.py` output shows the traceback; verified the store was untouched afterwards. All other refusals verified clean: pending local changes → ABORT; per-key remote → ABORT; missing group object with incomplete local file → `ABORT: load_items() failed for 1 object(s): ['_group_0']`; dry run → CHECK PASSED; `--delete` → 0 objects remain.
- **(b)** Not checked: `--catalogue`/`--backup-key` paths (read only); format-1 remotes.

### 3. [simplicity — verified by mutation] The `copy_remote` storage-kind branch is not observably different from the old manifest branch on reachable states

I reproduced the author's claimed mutation survivor: changing `if self.storage_kind != utils.STORAGE_PER_KEY:` (`remote.py:501`) to `if src_manifest:` passes all 57 tests in the three copy_remote-covering suites. I can also explain **why** it survives: a per-key remote's manifest is always `{}` (falsy → per-key branch either way), and a grouped remote with an emptied manifest has an emptied index too (both branches yield no children). The two implementations diverge only on corrupt states. The change is still right — it names the actual invariant — but it is a clarity fix, not a behavior fix. Corollary: `test_copy_remote_of_an_emptied_grouped_remote` (`test_write_order_groups.py:495`) does not distinguish the two implementations; its docstring ("branches on the storage kind, not on the manifest") claims more than the test can detect. If you want the branch pinned, the test needs a corrupt-state fixture (non-empty index, empty manifest).

- **(a)** Executed: the mutation run above, plus reading both implementations.
- **(b)** Not checked: nothing further — the equivalence argument covers all legitimate states.

### 4. [simplicity — verified] `committed_delete_keys` staged removal is belt-only

The push deletes journaled keys from the staged index bytes (`utils.py:1675, 1691`) and again from the live sidecar post-commit (`utils.py:1809`), but a deleted key can never be in the sidecar at push time: `__delitem__` removes the sidecar entry immediately (`main.py:1373-1374`), and both replay sites — open (`main.py:632-635`) and pull handle-swap (`main.py:1133-1135`) — strip journaled deletes from any freshly fetched index. Both replay sites are themselves mutation-tested (below). So ~6 lines across two sites could go. I'd keep them: they're the belt for exactly the invariant the replays guarantee, and removing belt from a delete path is a bad trade for 6 lines.

- **(a)** Executed/derived: source trace above; plus the replay-removal mutations (finding 6's run) confirm the replays are the load-bearing half.
- **(b)** Not checked: none — the reachability argument is complete for the public API.

### 5. [simplicity/compat — derived, not executed] cfdb `open_edataset` positional-argument shift

`group_bytes` now occupies the old `num_groups` positional slot (7th) in `open_edataset`. A caller passing positionally (`open_edataset(conn, path, 'w', ..., 8)`) silently gets `group_bytes=8` where 0.10 semantics were `num_groups=8` — bytes vs count, no error. Keyword callers are unaffected. Given `num_groups` is kept one release for compatibility, the safe move is to also keyword-gate both (or insert `group_bytes` after `num_groups`) for one release.

- **(a)** Derived: read the cfdb diff (signature + forwarding logic — the given-only forwarding semantics themselves are correct: explicit `None` = per-key on build, nothing on update = inherit).
- **(b)** Not checked: executed no legacy positional caller against new cfdb; downstream envlib / envlib-ingest-base reviewed by reading only.

### 6. [accuracy of the test suite — verified] Mutation spot-checks: 4 killed, 2 survivors, both survivors benign

I ran 6 mutants with `PYTHONDONTWRITEBYTECODE=1` (all restored; full battery re-run green afterwards — 203 passed):

| Mutant | Result | Killed by |
|---|---|---|
| Journal always stamps v2 (per-key file loses 0.10 readability) | **killed** | `test_journal.py::test_journal_version_follows_the_mode[None-1]` (`assert 2 == 1`) |
| Open-path delete replay removed | **killed** | `test_stale_incarnation.py::test_deletion_intent_survives_a_misfire` — but only by the *full* battery; my first 4-file run survived it, and I confirmed by script that the mutant observably resurrects a deleted key (`resurrected? True | keys = ['k1','k2']` vs `False` unmutated) |
| Pull-path (`_pull_remote_index`) delete replay removed | **killed** | `test_journal.py::test_replay_on_swap_prevents_resurrection` |
| Sidecar-layout rule: always discard mismatch | **killed** | `test_write_order_groups.py::test_offline_keeps_a_legacy_sidecar` |
| Sidecar-layout rule: never discard online | **survived** | — see below |
| `copy_remote` branches on manifest | **survived** | — finding 3 |

The never-discard survivor: I could not construct a reachable state where the online discard matters — any remote mode change implies a uuid change or a lagging `remote_ts`, both of which trigger other machinery (UUID refusal / full re-fetch). So it is untested belt, not a hole. Note for the author's process: my first attempt at mutant 2 "passed" only because I guessed wrong test filenames — when mutating, always run the complete battery, not a chosen subset.

- **(a)** Executed: all six runs above, commands and outputs as shown in this session.
- **(b)** Not checked: the author's full 25-mutant run (taken on trust); these 6 were my own selection targeting doubted guards.

### 7. [simplicity — minor] Remaining Q9 candidates, assessed

- **StaleIndexError belt** — keep. Finding 1 shows a real (benign) path around it; it is the only guard standing between a stale index and mistaken empty-group drops in the same-session window. Cost: ~10 lines.
- **fsck `empty_groups` / `dead_fraction` / `bad_entry_layout`** — keep all three. `empty_groups` catches crash-corner waste the old fields couldn't express; `dead_fraction` is the lazy-delete observability story and works against real transport (verified `content_length` is populated in both listing branches of the installed s3func 0.9.6 `response.py:248,299`). Minor overlap only: a group can't have `dead_fraction == 1.0` legitimately (push drops it), so the fields don't double-report.
- **Journal v1/v2 split** — keep. The compat constraint is real and I verified it by execution in both directions: a 0.11-written per-key file's v1 journal round-trips through real 0.10.5, and 0.10.5-written journals read back under 0.11. The split costs ~8 lines and one tri-state.
- **Sidecar-layout rule** (`main.py:576-587`) — keep as written; the offline exception is the subtle half and is tested. The online half is belt (finding 6).
- **`test_write_order_groups.py` at 605 lines** — keep. Every test I read targets a distinct invariant (allocation order, tail top-up, gid reuse, crash-after-commit, mode adoption, discard, republish...); splitting it would not make it simpler to reason about, only shorter.
- **`num_groups` shim** — as planned, one release; the int-raises-ValueError behavior is tested (`test_journal.py:425-433`).

## What was executed overall (for the record)

1. **Test suite**: 203 passed on the 12 fake-S3 suites (re-verified after every mutation restore).
2. **Fuzzing**: 8 seeds × 200 iterations, 2 concurrent writers (writes/updates/deletes/delete-then-reset/discard/partial-failure-retry/crash-after-commit), invariants: committed index↔manifest pairing, manifest objects exist, fsck clean, orphans only after crashes, key set == model, fresh-reader value verification. **All pass; no loss or corruption.**
3. **Cross-version interop with real 0.10.5**: per-key remotes writable and readable both directions (format stays '2'); journal v1 round-trips both directions; fsck clean.
4. **Migration script**: dry run, `--delete`, and four refusal conditions (above).
5. **Mutation spot-checks**: 6 mutants, results above.

## What was NOT checked (inherited assumptions)

- **Fuzzer limits**: both writers used the same `group_bytes`; no pull-failure injection (the tail-pull-failure scenario — writer without tail members local + injected group-GET failure — was **not** executed; the suite covers failed fresh groups but this path only by reading); no legacy remotes, no per-key mode, no metadata ops, no `prune`, no `copy_remote` in fuzz; crash simulated only at "commit succeeded"; the fake lock never contends; one `db_key` throughout.
- **Live-S3 suites** (the 8 listed files): not run — no credentials; their changed portions reviewed by reading only.
- **Downstream (cfdb/envlib/envlib-ingest-base)**: reviewed by reading diffs; the new `test_edataset_args.py` not executed by me.
- The author's **25/24 mutation claim** beyond my 6 spot mutants; `hydrate_and_delete.py`'s `--catalogue`/`--backup-key` paths; format-1 remotes.
- No timing, memory, or large-scale (>~100 keys / MBs) behavior — nothing about performance was measured and none is claimed here.

<!-- finished: 2026-10-07T10:13:25+13:00 exit=0 -->
<!-- openrouter: key string redacted 0 time(s) from the result, stream and stderr -->
<!-- openrouter (stream-visible, lower bound): 87 of 87 generations, providers={Sail Research:79,AkashML:8}, cost=$1.5188, prompt=7.66M cached=95% out=58.5k reasoning=39.3k -->
<!-- openrouter key usage: $1.5654 account-side ($36.8488 -> $38.4142; polluted by concurrent arms on the same key) -->
