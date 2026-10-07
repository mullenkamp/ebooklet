# Synthesis, code review `ebooklet-wog-code-2` (Fable 5.1, Gemini 3.1 Pro)

## Core

**One real problem: a data-loss path that A1 introduced.** A replacement push (`flag='n'`) commits, and
then the HEAD that A1 added fails. The journal has already been saved with the committed keys cleared but
`replace_pending` still set. The natural retry then purges every local key and commits an empty database.
I reproduced it in both modes (local and remote end with no keys).
- **Fix (Fable):** clear `replace_pending` in the same journal write as the committed keys, inside
  `update_remote`. The block in `Change.push` that does it later becomes dead.

**Refuted.** Gemini's headline claim that the migration recipe loses every hydrated key. I ran the real
sequence: 0.10.5's in-session `delete_remote()`, then the 0.11 open and push. All ten keys survive, because
an open against an absent remote downloads no index and so never reconciles. Fable independently ran the
real `hydrate_and_delete.py --delete` plus two republish routes, including a crash, and all were clean.

**Small, both arms:**
- validate the recorded `group_bytes` when it is loaded;
- `Catalogue.publish` still takes `group_bytes` in `num_groups`' positional slot.

**Pre-existing and minor:**
- a no-op push leaves one changelog file (as 0.10.5 did);
- `pull()` stamps freshness after a skipped reconciliation;
- the adoption of a remote that appears at push time skips the uuid check;
- some dead code.

---

## Proposed changes (nothing applied yet)

| # | Change | Source | Recommendation |
|---|---|---|---|
| R1 | `update_remote`: set `replace_pending` False with `clear_committed`, in the one `journal.persist`; delete `Change.push`'s now-dead `replace_pending` block; regression test (post-commit HEAD fails in a replacement push, both modes, retry keeps every key); mutant | Fable F1, verified | **do** |
| R2 | `remote._load_db_metadata`: a recorded `group_bytes` that is not an int in 1..`_MAX_GROUP_BYTES` is treated as unrecorded, with a warning; test; mutant | G5 / F3 | **do** |
| R3 | `Catalogue.publish`: `*` before `group_bytes` (the parameters after it become keyword-only); test | F2 (the OPEN_WORK proposal) | **do** |
| R4 | Drop the dead `and not index_fetch_suppressed` clause; replace `fetched_manifest is not None` with `index_fetched` (two sites) | F4 | do (tiny) |
| R5 | `Change.push`: clean up the changelog when there are no failures, not only on a commit | G4 (pre-existing, cosmetic) | your call; lean do |
| R6 | `pull()`: do not stamp freshness when the reconciliation scan was skipped (`reconcile_local_with_index` reports the skip) | G2 (pre-existing) | your call; lean do (small) |
| R7 | The pre-push adoption checks the remote's uuid against the local file before its forced pull, raising `UUIDMismatchError` as the open does | Fable O1 (pre-existing) | your call; backlog or do |
| — | Rejected: G3's "assign the session fields directly instead of a HEAD". That is the old `_note_commit`, which caused round 1's F1. With R1 a failed HEAD is safe (the retry hits the belt; a reopen re-fetches). | | no |
| — | Backlog note: booklet `n_keys` / `OverflowError` after a simulated hard crash (Fable O2, unverified) | | backlog |

## Reviewer table

| Arm | Wall | Commands run | Subagents | Cost |
|---|---|---|---|---|
| Fable 5.1 (high) | ~13 min | ran suites, 9 mutants, the migration script under 0.10.5, fault injection | none reported | Claude allocation |
| Gemini 3.1 Pro (high) | ~9 min | ran mutants and scripts; its G1 "verified" rested on a direct reconcile call, not the real path | 0 | free |

Fable found the only real defect, by fault injection on the exact window A1 changed.
