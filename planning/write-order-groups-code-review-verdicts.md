# Verdicts — round ebooklet-wog-code-1 (code frozen; no edits until all arms report)

## Gemini (gemini-3.1-pro-high), finished 09:49

| # | Claim | Tag | Verdict | How checked |
|---|---|---|---|---|
| G1 | The `StaleIndexError` belt is unreachable through the API: `remote_session.timestamp` changes only at open, a re-HEAD, or the session's own commit, and the open path already re-fetches whenever the cached state lags. A concurrent commit is caught by `lock.verify()` instead. Deleting the block fails only `test_stale_index_belt`. | simplicity | **accepted (read + arm ran)** | Consistent with the code: the belt compares two values from the SAME session, both set together by the fetch, the pull or `_note_commit`. It guards against an internal bug, not against remote state. It is a candidate cut. |
| G2 | A per-key remote written by 0.11 reads under 0.10.5 (format 2, 15-byte entries); no path mixes a stamp with the wrong entry size | accuracy | accepted (the arm executed the cross-version read; not re-run by me) | |
| G3 | Emptied groups are detected and GC'd, including deletes-only pushes and lost-key drops | accuracy | accepted (reasoning; consistent with the mutation results) | |
| G4 | `group_entry_size` matches `pack_group`'s layout | accuracy | **verified (read)** | `pack_group`: `>H` + key + 7-byte ts + `>I` + value. Non-ASCII keys: both sides use `len(key.encode())`. |
| G5 | The migration keeps metadata because `forget_remote_incarnation` *preserves* `remote_state.meta_section` | accuracy | **outcome verified, mechanism refuted** | `RemoteState.reset()` sets `meta_section = None` (`journal.py:273-275`). The metadata survives through `_build_meta_section_for_push`'s `remote_absent` branch, which embeds the LOCAL slot (`utils.py:1208`). The fixture test `test_republish_in_place_after_delete` asserts it. |
| G6 | Downstream forwarding and defaults are correct | accuracy | accepted (read) | |
| G7 | Keep the fsck fields and the `copy_remote` change | simplicity | noted | |

## Fable (claude-fable-5-1), finished 10:00. Randomized model check over 1,400 + 900 seeds; mutants; cross-version runs.

| # | Claim | Tag | Verdict | How checked |
|---|---|---|---|---|
| F1 | A republish that partly fails and is retried IN THE SAME SESSION loses the failed groups' keys (locally and remotely). `_note_commit` leaves `uuid` None, so push 2's pre-push re-check force-pulls and reconciliation deletes unjournaled local keys the new index lacks. A crash after the commit plus a reopen does the same. Pre-existing on 0.10.5, but the migration recipe walks every dataset through it. | accuracy, **data loss** | **verified (ran)** | `code1/f1.py same`: push 1 fails group 1, push 2 → local AND remote `['k0','k1','k4'..'k9']` (k2, k3 gone). `reopen`: all ten keys survive. Crash route not re-run (arm ran it). |
| F2 | `hydrate_and_delete.py` compares against the LOCAL sidecar, not the remote's index: a stale sidecar passes the check and the remote is deleted. The delete also runs in a second, unlocked session. | accuracy, data loss (low likelihood) | **accepted (read; the arm ran the script)** | The script uses `eb._remote_index`. Under 0.10.5, an open whose local stamp is ahead of the remote does not re-fetch, which is exactly the bug 0.11 fixes. Fix: GET and parse the db object fresh, and delete via `eb.delete_remote()` inside the locked session. |
| F3 | `del k; db[k] = v; del k` loses the delete (pre-existing; sibling of the discard bug) | accuracy, pre-existing | **verified (ran)** | `code1/f3.py`, both modes: journal empty after the sequence; push 1 is a no-op and the remote still serves `k`; an unrelated push 2 then drops it. |
| F4a | `db.group_bytes` reports grouped for an offline per-key reader cache | accuracy, cosmetic | accepted (the arm ran it) | Readers record no mode, so the default applies. |
| F4b | `PushResult.updated=True` when every group failed and nothing committed | accuracy, pre-existing | accepted (read: `Change.push` returns `updated=not replace_pending` on any partial failure) | |
| F4c | The open path never stamps the local file after a fetch, so a reader re-GETs the whole db object on every open after one remote change | perf, pre-existing | accepted (the arm ran it; consistent with `_init_common`, whose stamping happens only in `_pull_remote_index`) | |
| F4d | A hydrated file republishes in download order, not original write order | locality | accepted. Matters only when a file was incomplete before hydration; the WRF archive files are complete. | |
| F4e | `group_bytes` took `num_groups`' positional slot: an old positional `num_groups=101` becomes a 101-byte target | accuracy, hazard | **verified (read)** | The signatures put `group_bytes` where `num_groups` was. Fix: keyword-only. |
| F4f | ingest-base update helpers against an ABSENT remote now create grouped | low | accepted | |
| F5 | Eight behaviours are unpinned (their mutants survive): tail size from updated local lengths; the pull-time remote_ts re-fetch; the pull-time legacy raise; adoption's journal write; the suppressed clause of the layout rule; the 19-byte belt; the emptied loop's `updated`; the lost-key emptied branch | tests | accepted (the arm ran them; I did not re-run) | Needs tests, or deletion of the dead branches (S1). |
| S1 | Dead delete machinery: `committed_delete_keys`, the `key in deletes` filter, the `if not keys_in_group` branch, the commit-time `set_storage`, `STORAGE_LEGACY_HASH` | simplicity | accepted (reasoning verified; the arm's 550-seed equivalence run) | Deletes are already gone from the sidecar before the push; every affected group has a locally present member, so a lost-key drop cannot empty it. |
| S2 | Replace `_note_commit` with `_load_db_metadata()` (fixes F1's main route) | simplicity + accuracy | **accepted, tested by the arm (203 pass)** | |
| S3 | `StaleIndexError`: keep the check as an internal error, drop the public class | simplicity | accepted. Gemini G1 says delete it outright. | |
| S4 | Journal: always write v1 plus the `storage` field; a 0.11 grouped READER cache has no journal anyway, so v2 protects only writer files | simplicity | accepted (the arm ran 0.10.5 on both file kinds). Reverses Mike's "grouped only" bump; his decision. | |
| S5 | cfdb and envlib: drop the `_NOT_GIVEN` sentinels and let `group_bytes` ride `**kwargs` | simplicity | accepted; a style decision (loses the named parameter) | |
| S6 | `plan_groups`: return only the assignment | simplicity | **verified (read)**: the caller discards `_fresh_gids`, `_tail_used` | |
| S7 | `bad_entry_layout` has no producer; keep `dead_fraction` | simplicity | noted | |
| S8 | Larger redesign: stop editing the sidecar in `__delitem__` and hide deletes through the journal. Removes both replays and discard's re-pull, and fixes F3 | design | backlog candidate | |
| Sound | Cross-version per-key read and write under 0.10.5; randomized model check with 0 failures (F1/F3 masked); write order holds at 2,000 keys | | accepted (the arm executed them) | |

## GLM-5.3 (z-ai/glm-5.3:exacto), finished 10:13, $1.57. A 2-writer fuzzer (8 seeds × 200 iterations), cross-version runs, 6 mutants, migration-script refusals.

| # | Claim | Tag | Verdict | How checked |
|---|---|---|---|---|
| L1 | A retry in the SAME session after a crash-after-commit (exception, process alive) re-commits from the stale cached manifest; the crashed commit's generations become invisible orphans. Benign; the `StaleIndexError` docstring's "unreachable" overstates it | accuracy, low | accepted (reasoning consistent: the journal is not cleared, so the retry re-pushes the same keys; nothing reader-visible is lost) | Not realistic for a real crash (the process dies). With S2 the session state would still be pre-commit, so the outcome is unchanged. |
| L2 | `hydrate_and_delete.py` raises a raw `UnsupportedFormatError` traceback on a format-3 remote (nothing deleted) | accuracy, UX | accepted (arm ran it) | Trivial fix. |
| L3 | The `copy_remote` survivor reproduced; the test's docstring claims more than it can detect | simplicity | agrees with my survivor note | |
| L4 | `committed_delete_keys` is belt-only (agrees with Fable S1) but KEEP it as a belt | simplicity | noted; disagrees with Fable on keep vs cut | |
| L5 | Positional shift in cfdb `open_edataset` (agrees with F4e) | compat | verified (read) | |
| L6 | 6 mutants: 4 killed; the "never discard online" survivor is benign belt | tests | accepted | Overlaps Fable F5. |
| L7 | Keep `StaleIndexError`, the fsck fields, the journal split, the test file; ran the per-key cross-version in both directions | simplicity | noted; disagrees with Gemini (delete the belt) and Fable (demote it; always v1) | |
| — | **Did not find F1** (its fuzzer's partial-failure retries used journaled writes, not a hydrated republish) | | | Clause (b) lists "no legacy remotes" in the fuzz. |

## Reviewer table

| Arm | Wall | Commands (`arm_activity.py`) | Subagents | Cost |
|---|---|---|---|---|
| Fable 5.1 (high) | ~43 min | see below | — | Claude allocation |
| Gemini 3.1 Pro (high) | ~32 min | see below | 0 | free |
| GLM-5.3 exacto (high) | ~56 min | see below | 0 | $1.57 |
