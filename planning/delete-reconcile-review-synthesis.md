# Synthesis — delete-reconcile dual-blind plan review (2026-07-20)

Reviewers: Gemini (agy, Mike-driven, neutral-worded brief) + fresh blind Claude subagent
(adversarial twin brief). Same scope (`~/git/ebooklet` 0.10.1 + `~/git/booklet` 0.12.8,
`~/.envlib` excluded); neither saw the other's findings. Both verdicts: **implementable
with changes**. Full reports: `delete-reconcile-review-{gemini,claude}.md`; brief:
`delete-reconcile-review-brief-gemini.md`. Wording divergence per the standing agy
protocol; the briefs were substantively identical.

## Convergent (adopted)

- **Reconcile-scan concurrency crash** (Gemini MAJOR-2 ≡ Claude MAJOR-2a): fetch workers
  complete `local_file.set()` outside `_index_lock`; booklet's mutation guard raises
  RuntimeError mid-scan — a NEW failure mode for pull()/get(). Remedy adopted: Claude's
  retry-once-then-skip-with-log (bounded; a skipped reconcile is the pre-fix status quo)
  over Gemini's unbounded retry loop.
- Every factual design claim verified by both, to the same lines — including the
  `create_changelog` resurrection hazard and the remote_ts-not-file-timestamp choice.

## Disagreements and how they resolved

1. **Gemini's NOTE ("loss window broader than claimed — no skew needed")** — checked
   against source, does NOT hold: `_pull_remote_index` calls `sync()` first (persisting
   the journal), so an API write is journal-protected on reopen; a crash before any sync
   leaves the write's stamp NEWER than the previous ingest → ts-guard keeps it. The
   surviving form of the concern is Claude MAJOR-1 (user-supplied timestamps), ruled
   below.
2. **Open-path gate** — Gemini MAJOR: fetched-only leaves upgraded pre-fix caches able to
   republish residue (recommended unconditional open reconcile). Claude finding 17:
   fetched-only is provably sound POST-fix. Not actually contradictory: steady state
   sound, upgrade transition holed. **Mike's ruling (R2): keep fetched-only**; the
   transition is a documented note (changelog + pull() docstring). Post-implementation
   observation: open-path-only reader caches never advance their file stamp, so they
   refetch (and reconcile) on EVERY open — the transition limitation in practice affects
   only writer/pull-synced caches. The live incident cache healed on first open under the
   fixed tree.
3. **Re-materialization race** — Gemini rated it tolerable ("heals on next pull"); Claude
   rated it MAJOR (the next pull never comes while the remote is idle — freshness gate).
   **Mike's ruling (R3): close it in code** — fetched keys are membership-re-checked
   against the current index (under `_index_lock`) before the read operation returns
   (implemented post-completion rather than pre-set: strictly stronger ordering — it
   checks against the newest index — and avoids threading callbacks into the utils fetch
   functions).

## Claude solo findings (all adopted)

- **MAJOR-1 user-supplied timestamps** defeat the ts discriminator in both directions.
  Grep-verified: nothing in the stack passes explicit timestamps (cfdb/envlib all default
  write-time stamps). **Mike's ruling (R1): document the constraint** + pin both
  directions with tests (future-stamped key survives; backdated unjournaled write is
  deleted); the membership-leg redesign declined as complexity not currently warranted.
- MINOR-3: check-1 rationale corrected (booklet iterators already skip reserved keys —
  the check is belt); the **metadata twin defect** (remote metadata deletion never
  propagates) filed as a new `[ebooklet]` backlog item.
- MINOR-4: "heals at explicit pull()" claim corrected (freshness gate early-returns on an
  unchanged remote).
- MINOR-5: mass-purge log escalation (warning when >25% of local keys deleted). Adopted.
- NOTE-6: freshness-gate clock-skew hole + stale-writer-push scenario → new `[ebooklet]`
  backlog item (pre-existing, single-producer commons immune).
- NOTE-7: `timestamps()` single-pass scan adopted; open-path file-timestamp behavior →
  backlog note.
- NOTE-8: write-vs-delete conflict semantics (last-writer-wins toward the write) pinned
  by an asserted test.
- NOTE-9: all suggested test additions adopted (18-case matrix → 25 collected tests).

## Outcome

Implemented as `ebooklet 0.10.2` (see CHANGELOG). ebooklet suite 190/190 incl. live; the
preserved live incident cache (`incident-stale-cache-20260720.tar.gz`) converges on first
open under the fixed tree (the retracted envlib-commons streamflow entry reconciled away,
esa-sst intact); cfdb + envlib suites run against the patched tree pre-release.
