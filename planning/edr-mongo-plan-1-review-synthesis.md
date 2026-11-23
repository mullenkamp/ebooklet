# Plan review `edr-mongo-plan-1`: synthesis

_Round run 2026-11-23 on `mongo-read-path-plan.md` (the pre-review plan is frozen inside
`edr-mongo-plan-1-review-brief.md`). Arms: Fable 5.2 (effort high) and Sonnet 5.5 (effort xhigh), with
the same brief and the same scopes (the ebooklet, cfdb and envlib repos). Their reports are
`edr-mongo-plan-1-review-fable.md` and `edr-mongo-plan-1-review-sonnet.md`. Mike adopted all 13
proposed changes on 2026-11-23, and the revised plan is `mongo-read-path-plan.md`._

## Core

Both arms executed rather than read. Neither could run plan code, so each transcribed Steps 3–4 and
drove the transcription against real ebooklet pushes with GC, ebooklet's real fetchers, the fake S3,
real cfdb and a real `mongod`.

**Confirmed sound in both runs:**
- the fetchers work through a reader that skips the HEAD;
- the refresh converges after a crash at every step;
- no path stores bytes under a wrong label;
- the store contract covers every cfdb read path.

**The same five defects, found independently by both arms:**
1. **A deleting refresh during a request gives silent NaN or wrong coordinates.**
2. **The refresh stalls for good on a backwards commit stamp.**
3. **The 404 rule needs the applied stamp read at construction.**
4. **A memo per request is needed.**
5. **Nothing clears `missing`.**

All five are fixed in the revised plan.

| Arm | Commands run | Subagents | Wall time |
|---|---|---|---|
| Fable 5.2 | 45 | 0 | 20 min |
| Sonnet 5.5 | 70 | 0 | 22 min |

## Verdicts

| Finding | Fable | Sonnet | Verdict | How often |
|---|---|---|---|---|
| A deleting refresh mid-request gives silent NaN or wrong coordinates | F1 | F2 | **Verified.** Both streams show it executed on real cfdb. | Rare: routine ECan `prune()` is local-only, so only repairs, truncates and variable deletes remove remote keys. |
| A backwards commit stamp stalls the refresh | F2 | F1 | **Verified (read):** the stamp is the wall clock (`utils.py:1804`). Both arms executed a skewed push. | Rare: clock skew or an NTP step. |
| The 404 rule needs the construction-time stamp, and a refetch before a fault | F3 | F4 | **Verified** on both transcriptions. | Uncommon: a refresh during a request. |
| A memo per request (2 round trips per `get` without one) | F6 | F3 | **Verified:** ×4.4 on loopback. | Every request |
| `missing` is never cleared | F4 | F5 | Verified (a plan omission) | Rare |
| A second worker can interleave | F5 | reasoned | Accepted (reasoning) | Rare |
| Writer order (per-key delete, `delete_remote`) is not pinned by ebooklet's tests | — | F4 | **Accepted, not verified** (Sonnet ran it). The remedy is new tests, which are mutation-checked. | n/a |
| MongoDB traps: int/long validators, TTL needs a Date, an oversized doc breaks the batch, batch huge `$in` lists | F9 | F6 | **Verified** by both. The arms differed on whether the rest of the batch survives an oversized doc; pre-filtering covers both cases. | n/a |
| Mutant and test gaps | F7 | F7 | Accepted (the union) | n/a |
| Contract loose ends: xarray, `copy()`/`prune()`, `map()` loads first | F8 | Q8 | Verified | n/a |
| Uncommitted per-key bytes are served, as in ebooklet | F10 | labels | Verified | Rare |

## Changes adopted (all 13, Mike 2026-11-23)

1. Skip a refresh only when (uuid, ts) is **equal**; write the applied pair last.
2. A `rev` counter, bumped in apply step 2. An absent entry triggers one re-read of the dataset doc;
   a changed `rev` raises `StaleIndexError`. Step 4 is conditional on `rev`.
3. A memo per request, filled by `load_items` and served by `get`.
4. The 404 path compares with the **construction-time** stamp. The back-off is replaced by: re-read
   the entry, refetch once, then one HEAD.
5. Clear `missing` on every HEAD 200.
6. MongoDB: `['int','long']` integers; `t` a required Date; pre-filter by size; tolerate only error
   11000; batch `$in`.
7. Writer-order tests, using a probe at the commit PUT.
8. The union of the test lists. A table-driven `storage_kind` test goes before the refactor. Drop the
   "GET-stamp" mutant.
9. A hermetic cfdb-through-store test in ebooklet's suite.
10. One shared dataset-type dispatch helper in cfdb.
11. Drop the xarray claim. Clear errors from `prune()`/`copy()`. Document that `map()` loads first.
12. Drop the `s` field and the `$max` on `checked_at`.
13. Carry to plan 2: the uncached endpoint must also cover per-key 404s, and writers must commit
    before they delete.

**Not adopted:**
- delaying GC to fsck's age gate (Sonnet): it changes writers;
- making stamps monotonic at the writer (Fable): change 1 makes it unnecessary.
