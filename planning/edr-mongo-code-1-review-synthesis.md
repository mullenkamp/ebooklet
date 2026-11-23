# Code review `edr-mongo-code-1`: synthesis

_Round run 2026-11-23 on the uncommitted working trees of ebooklet (`ebooklet/mongo/`, the `remote.py`
refactor, new tests) and cfdb (`open_store_dataset`, the shared dispatch). Arms: Fable 5.2 (effort
high) and Sonnet 5.5 (effort xhigh), with the same brief (`edr-mongo-code-1-review-brief.md`) and the
same scopes. Their reports are `edr-mongo-code-1-review-fable.md` and `-sonnet.md`. Mike ruled on the
remedies on 2026-11-24, and the fixes are recorded in `mongo-read-path-plan.md`, "Implementation
notes"._

## Core

Both arms executed throughout, and neither used subagents. Fable took 32 minutes and added 32 mutants
of its own. Sonnet took 53 minutes, with a 20-configuration interleaving matrix, 24 crash points, a
real HTTP server and 16 threads sharing one session.

**Confirmed sound with one refresher:** the heal path, the 404 classification, crash convergence, the
memo, the `remote.py` refactor, the cfdb dispatch refactor and the validators. Both arms re-ran the 25
original mutants, and all were killed.

**Three real problems:**
1. **Two overlapping refreshes diverge for good.** Both arms reproduced it, with different schedules.
2. **A request that overlaps an apply decodes new bytes with old metadata.** The values, and the
   coordinates, come from neither commit, silently. Both arms reproduced it; Sonnet showed that a
   `flag='n'` replacement keeps the uuid, so checking the uuid is not enough.
3. **A streaming `HttpSession` passed to `object_reader` fails, then hangs** (Sonnet).

| Arm | Wall time | Subagents | Notable experiments |
|---|---|---|---|
| Fable 5.2 | 32 min | 0 | Two-refresher schedule, re-encode mid-request, per-key mid-push read, 32 extra mutants (18 survived), a refactor differential |
| Sonnet 5.5 | 53 min | 0 | Two schedules of two refreshers, `delete_remote` and `flag='n'` re-encode, HttpSession over a real HTTP server, an interleave matrix, 24 crash points, 11 surviving mutants |

## Verdicts

| Finding | Fable | Sonnet | Verdict | How often |
|---|---|---|---|---|
| Overlapping refreshes diverge for good; the `ops.md` claim that they converge is false | F1 | F2 | **Verified:** I traced it against `refresh.py`, and both arms executed it | **Rare:** needs a second refresher, which is not planned |
| Old metadata with new bytes: wrong values and coordinates, silently | F2 | F1 | **Verified:** both arms executed it | **Rare:** a replaced remote, or a variable re-encoded in place, during a request |
| Per-key remotes mix cached old chunks with chunks overwritten in place, for the whole upload-to-refresh window | F3 | — | Executed | **Essentially never today:** both per-key catalogued datasets are static; checked by HEAD on 2026-11-24 |
| Absent keys during any refresh give `StaleIndexError` | F4 | F3 | Executed by both | Rare |
| A streaming `HttpSession` gives a TypeError, then a hang | — | F4 | **Verified (read):** s3func `http_url.py:109` defaults to `stream=True` | **Ordinary** for anyone following the docstring |
| Test gaps (Fable 18/32 and Sonnet 11 mutants survived); both arms then drove those paths with the real code, and all behaved | F5 | F5 | Executed by both | n/a |
| 13–19 MongoDB round trips per warm request | F6 | Q8 | Executed by both | Every request |
| Nits: dead `_entries`, the `get_metadata` edge case, `ensure_schema(True)`, long lines, primary reads, stale store sticky | F8 | smaller | Verified where checked | — |
| Outside this change: cfdb `flag='n'` over an existing remote reads an old chunk (Fable) and drops attrs keys (Sonnet) | F8 | smaller | Not checked | Backlog |

## Remedies (Mike, 2026-11-24)

| # | Remedy | Status |
|---|---|---|
| 1 | Concurrent refreshers are **not planned**. "Exactly one" is documented as a hard requirement, with the design concurrent refreshers would need (conditional per-entry writes or a fenced lease). No lease is built. | done (docs) |
| 2 | Any refresh overlapping a request → retry: a claim-first `rev` and `applying`, construction refusal, `MongoStore.verify()` | done |
| 3 | A per-key timestamp mismatch raises `StaleIndexError` | done |
| 4 | `object_reader` refuses a streaming session | done |
| 5 | Tests for every surviving mutant, plus regressions from the arms' experiments; re-mutate | done (53/53 killed) |
| 6 | Cleanups and the `ops.md` additions | done |
| 7 | Reduce the request round trips | **deferred to plan 3** |
| 8 | Backlog items: the cfdb `flag='n'` replacement problems | recorded in envlib `OPEN_WORK.md` |
| — | A forced re-diff safety net | **documented, not built** (Mike, 2026-11-24: build it if divergence is ever seen); the design and its cost are in `docs/ops.md` |
