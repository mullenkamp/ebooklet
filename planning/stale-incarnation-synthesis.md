# Stale-incarnation round — synthesis (2026-09-08)

Round `ebooklet-stale-incarnation-1`: Opus 5 and Gemini 3.1 Pro, same brief, same code (0.10.4 at
`7bf5ca9`), scopes `ebooklet` + `cfdb`. The brief is the design as first drafted; the arms were
asked to stress its invariants and to run experiments. Every finding below was reproduced by the
author with a switchable prototype before being adopted; none was taken on the reviewer's word.

## What the review changed

| decision | as drafted | as shipped | why (verified) |
|---|---|---|---|
| Forget step | unlink sidecar, reset remote state | same, plus `_set_file_timestamp(0)` | One spurious HEAD 404 stranded the file: the next open saw the sidecar present and the stamp unchanged, never re-fetched, and the next push silently truncated the remote to the locally-held keys (per-key: fsck showed orphans; grouped: nothing flagged). With the stamp reset the next open re-fetches. |
| Journaled deletes | cleared | kept | Clearing cancelled a pending deletion after a misfire; the incident repro is byte-identical without it (empty sidecar pulls nothing; per-key deletes target keys the new incarnation cannot hold). |
| Metadata for a rebuilt remote | persist `meta_pending` at open | push-time fallback (`_build_meta_section_for_push(..., remote_absent=...)`) | The latch made a misfired writer republish stale metadata over another writer's newer version, forever. The fallback still carries metadata onto a genuine rebuild. |
| Governing rule | — | forget only what the remote can re-derive | The three findings above are one rule. It is the helper's docstring. |
| Tests | 9 | 19 (three misfire tests, grouped twin of the crash-recovery guard, honest labels on guards that also pass on 0.10.4) | The misfire tests fail on the draft and pass on the shipped fix; the incident tests fail on 0.10.4. |
| Docs | rebuild semantics | plus: a cold cache rebuilds a near-empty remote; readers' key listings never heal; `num_groups` change needs `'n'` | Reader listing behaviour verified in both storage modes. |

Untouched by the review: the diagnosis, the placement in `_init_common`, the flag matrix, the
`replace_pending` carve-out, the same-session `delete_remote()` change.

## The arms disagreed, and the disagreement was the finding

Gemini marked the deletes-drop and the metadata latch as verified. Its verification covered the
incident path only. Opus constructed the misfire path (a 404 while the remote is alive) and both
claims fell. Agreement between arms on the happy path was not corroboration of the unhappy one.

Accepted on trust after the round: the S3 provider status taxonomy (which conditions answer 404
rather than 403/5xx) — not something a fake store can settle; the live end-to-end check on a
scratch B2 key covered the delete-and-rebuild path only.

## Code review — round `ebooklet-stale-incarnation-code-1` (2026-09-08)

Same arms, same discipline, on the implemented 0.10.5 diff (uncommitted working tree) with the
design record in scope. Both arms built an environment, ran the suite and mutated the code. Every
finding below was reproduced by the author before adoption.

| # | finding | severity | verdict | what changed |
|---|---|---|---|---|
| O-F1 | A session whose open saw a TRANSIENT 404 and which then pushes replaced the live remote's index with its own keys only; fsck called the result healthy and the lost values orphans | critical | **verified** (both storage modes) | `Change.push` re-HEADs once before pushing when the session believes the remote absent; if it exists, `_pull_remote_index(force=True)` adopts it (fresh index + reconciliation) and the push merges. Test `test_misfired_session_push_adopts_the_live_remote`. |
| O-F2 = G-1 | `reset()` dropped `remote_ts`, so the healing re-fetch skipped reconciliation (`prev_synced_ts` None); a key deleted remotely in the window was served locally and RE-PUSHED. Reachable through the change's own scenario (delete, second writer opens in the window, owner rebuilds without a key) — no 404 anomaly needed | high | **verified** (both arms, both modes, author A/B) | `RemoteState.reset()` keeps `remote_ts` — it is the file's reconciliation watermark, not a cache of the remote. Tests `test_remote_deletion_during_misfire_window_is_reconciled`, `test_second_writer_in_deletion_window_does_not_resurrect`. The five original assertions of `remote_ts is None` were pinning the defect. |
| O-F3 | Docs/CHANGELOG claimed "never data" and "reversible by construction" unconditionally; "any local file" ignored the `replace_pending` carve-out; "pending deletes … pushed" overstated | medium | verified (text) | Both rewritten after F1/F2 made them true. |
| O-F4 / G-2..5 | Mutations not caught: the moved `RemoteState.load` (the helper persists first, so a duplicate load is harmless — comment was wrong), the reserved-key filter (dead: the sidecar never holds reserved keys), `sync()` after the purge (belt), the six-field session reset, the `meta_section is None` half of the fallback (unreachable belt) | medium | verified | Comment corrected; purge replaced by `_remote_index.clear()` (O(1), keeps layout — both arms); six-field reset now unit-tested; belt kept and labelled. |
| O-F5 | `delete_remote()` now mutates session state a concurrent push reads, with no `_push_active` guard | low–medium | verified (code read) | Guard added, `PushInProgressError` like `prune()`/`clear()`; test. `keys()`'s unlocked two-phase iteration documented as the caveat. |
| O-F7 | The forget block rewrote the header stamp and logged "no longer exists" on every open of a never-created remote | low | **verified** | Gated on the file having been in sync with a remote (`remote_ts`/manifest/meta); sidecar unlink stays unconditional (covers the orphan-sidecar shape). Test `test_forget_is_quiet_on_a_never_created_remote`. |
| O | Refuted its own Q2b hypothesis: the push-time fallback embeds no metadata a first push of a fresh database did not already embed on 0.10.4 | — | accepted (arm ran both versions) | — |
| O | 12 vs 13 failures on full revert in the author's notes | — | verified: the 13th was the reader guard referencing a new helper by name | Guard now uses a literal path. |

Mutants of the code-review changes (each applied alone; new module): re-check removed → 2 fail;
`reset()` drops `remote_ts` → 9 fail; forget gate always on → 1 fails; push guard removed → 1
fails. Final: new module 28 passed; hermetic tier 156 passed; full suite including the live-B2
tier 223 passed (1 scale test deselected as configured); live end-to-end on a scratch B2 key clean
on the final code.
