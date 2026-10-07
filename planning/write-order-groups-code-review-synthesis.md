# Synthesis — code review ebooklet-wog-code-1 (Fable 5.1, GLM-5.3, Gemini 3.1 Pro)

## Core

**What held:** all three arms found the write-order mechanism sound, and each checked it by execution:
- write order holds at 2,000 keys;
- Fable's randomized model check over 2,300 runs and GLM's 2-writer fuzzer found no other failures;
- per-key remotes round-trip with 0.10.5 in both directions (format 2, journal v1);
- `group_entry_size` matches `pack_group`;
- gid reuse is safe.

**What must change (verified by me):**
1. **F1, data loss in the migration path (pre-existing, made likely by the recipe).** A republish that
   partly fails and is retried in the same session loses the failed groups' values, locally and remotely:
   the session does not know its own commit created the remote, force-pulls, and the reconciliation
   deletes the unjournaled hydrated keys. A crash after the commit followed by a reopen does the same.
   - Fix: refresh the session from the remote after a commit (`_load_db_metadata()`).
   - Fix: journal every key of a push that creates the remote, so the reconciliation guard protects them.
2. **F2, the migration script.** It checks against the local copy of the index (a stale copy passes)
   and deletes in an unlocked second session. Fix: compare against a fresh GET of the db object, and
   delete inside the checked, locked session. Also refuse a format-3 remote cleanly (L2).

**Smaller (verified or accepted):**
- `group_bytes` took `num_groups`' positional slot (make it keyword-only).
- Pre-existing:
  - `del k; db[k] = v; del k` loses the delete (F3; backlog with the journal-based delete redesign, S8);
  - `PushResult.updated` is True when nothing committed;
  - readers re-download the index on every open after a remote change.
- Eight unpinned behaviours: add tests, or delete the dead ones (S1).

**Where the arms disagree (your call):**
- `StaleIndexError`: delete it (Gemini), demote it to internal (Fable), or keep it (GLM).
- The dead delete-handling code: cut it (Fable) or keep it as a belt (GLM).
- Always write journal v1 (Fable) or keep the grouped-only v2 split (GLM; your earlier decision).

**Refuted:** Gemini's mechanism for the migration keeping metadata (G5). The outcome is right but the
cause is different.

---

## Proposed changes (nothing applied yet)

| # | Change | Source | Recommendation |
|---|---|---|---|
| A1 | After a successful commit, `remote_session._load_db_metadata()` replaces `_note_commit` (the session knows its remote exists; one HEAD per push) | F1/S2 (Fable tested: 203 pass, retry keeps all keys) | **do** |
| A2 | A push to an ABSENT remote journals every changelog key as written before uploading (cleared per committed group), so the reconciliation after a crash or re-pull cannot delete the failed groups' hydrated values | F1 crash route | **do**; tests for both routes from the 0.10.5 fixture |
| A3 | `hydrate_and_delete.py`: compare against a freshly GET-parsed db object; delete via `eb.delete_remote()` inside the locked session; clean ABORT on a non-legacy remote (including format 3) | F2, L2 | **do**; re-run the script tests |
| B1 | `group_bytes` keyword-only in `open_ebooklet`, `open_rcg` and `open_edataset` (and the parameters after it) | F4e, L5 | **do** |
| B2 | Tests for the unpinned behaviours that stay: tail size from updated lengths; the pull-time remote_ts re-fetch; the pull-time legacy raise; the adoption's journal write; the suppressed clause of the layout rule; then re-mutate | F5, L6 | **do** |
| B3 | `plan_groups` returns only the assignment | S6 | **do** (trivial) |
| C1 | Delete the dead delete machinery: `committed_delete_keys`, the `key in deletes` filter, the `if not keys_in_group` branch, the commit-time `set_storage`, `STORAGE_LEGACY_HASH` | S1 vs L4 | **your call**; I lean to cutting the clearly dead parts and keeping the `keys_in_group` branch as an assert |
| C2 | `StaleIndexError` | G1 / S3 / L7 | **your call**; I lean to Fable's: keep the check, make it a plain internal `RuntimeError` |
| C3 | Journal always v1 plus `storage` | S4 vs L7 | **your call**; you chose the grouped-only bump; GLM verified it works as built |
| C4 | Drop the cfdb/envlib `_NOT_GIVEN` sentinels in favour of `**kwargs` | S5 | lean **no**: B1 removes the hazard, and the named parameter documents itself |
| D1 | Pre-existing `PushResult.updated` when nothing committed | F4b | small fix; your call (in scope or backlog) |
| D2 | Pre-existing: the open path does not stamp the local file after a fetch (readers re-GET every open) | F4c | small fix; your call |
| D3 | `db.group_bytes` for offline per-key caches: infer from the sidecar layout | F4a | small fix |
| E1 | `del k; db[k]=v; del k` loses the delete, and the journal-based delete redesign | F3, S8 | **backlog** (pre-existing; a design change) |

## Reviewer table

| Arm | Wall | Commands (`arm_activity.py`; excludes subagents) | Cost |
|---|---|---|---|
| Fable 5.1 (high) | ~43 min | 28 (4 build/execute), plus subagent runs (fuzzer, mutants) | Claude allocation |
| Gemini 3.1 Pro (high) | ~32 min | 172 (84 unique) | free |
| GLM-5.3 exacto (high) | ~56 min | 78 (12 build/execute) | $1.57 |

Fable found both data-loss paths. GLM's fuzzer and Gemini's runs missed F1 because neither exercised a
hydrated republish with a partial failure.
