# Task: critical architecture assessment of ebooklet

You are an independent, fresh-context reviewer conducting a **design-level architecture
assessment** of ebooklet (with booklet in scope as its storage dependency). This is not a
bug hunt and not a style review: the question is whether the architecture is the right one
to carry the next phase of this stack's life, and where it is not, what should change and
when. Verify every claim in this brief against the real source, run local experiments where
they settle questions, and challenge the brief's own framing wherever the evidence points
elsewhere. Do not aim to confirm; a report that only agrees is a failed report.

## The system and why this assessment is happening now

Stack (all by the same author, pure Python):

- **booklet** (`~/git/booklet`, v0.12.6) — single-file key-value store (dbm-style flags,
  hash-bucket index, thread + file locks). CHANGELOG.md records its recent hardening.
- **ebooklet** (`~/git/ebooklet`, v0.9.4) — THE PRIMARY SUBJECT. A booklet that syncs with
  S3-compatible object storage via **s3func** (`~/git/s3func`, v0.9.2, urllib3-only client
  with a bakery-style distributed lock). Local-first reads/writes; explicit `push()`;
  transparent per-key pulls; optional grouped storage (`num_groups`: keys hash into N
  packed S3 objects, ranged reads per member).
- **cfdb** (`~/git/cfdb-repos/cfdb`, v0.9.1) — CF-conventions array database whose chunks
  live in a booklet/ebooklet; **envlib** (`~/git/envlib-repos/envlib`, v0.1.1 staged) — a
  distributed environmental-data catalogue: datasets are cfdb files behind ebooklet
  remotes, and the catalogue itself is an ebooklet RemoteConnGroup (RCG) whose entries
  point at member datasets.

Two events make the timing load-bearing: envlib is about to put a **public** RCG commons
on Backblaze (the RCG entry schema is a never-change public surface once consumers exist),
and a large **tethys→envlib data migration** will push a high volume of data through
exactly the paths assessed here. Architectural change becomes an order of magnitude more
expensive after both. Assess accordingly: recommendations should be phased against those
two milestones.

## Evidence base — read this before forming views

July 2026 produced an unusually dense, well-documented failure history. Read, at minimum:

- `~/git/ebooklet/CHANGELOG.md` — 0.8.4 through 0.9.4: push-integrity fixes, the
  `flag='n'` contract's three-release evolution, `Change.pull()` repair, the 404-integrity
  re-check protocol (`RemoteIntegrityError`), and the `flag='n'` second-push **data-loss**
  bug (every push in an 'n' session re-wiped the remote; present since 0.9.0; caught only
  when 0.9.3's loudness made a downstream test fail).
- `~/git/booklet/CHANGELOG.md` — the 0.12.6 iterator-contract release (lock-across-yield
  deadlock; per-step locking + mutation counters) and the fixed-format write guards.
- `~/git/envlib-repos/envlib/OPEN_WORK.md` — the Done section chronicles every round with
  verification detail; the Backlog holds the KNOWN-OPEN items relevant here, including:
  timestamp-based change tracking silently ignoring clock-skewed local edits (deferred
  since 0.8.4; "real fix needs modification tracking independent of timestamps"); no
  offline mode for unreachable remotes (envlib works around it by reading the local cache
  file directly); the accepted mid-push false-positive window in the 404 re-check.

## The author's three seam hypotheses — test them, narrow them, or overturn them

The failure pattern above clusters, in the author's reading, on three structural seams.
These are HYPOTHESES for you to evaluate, not the boundary of your scope.

**Seam 1 — session/lifecycle state.** A session's mode (creating vs replacing vs updating
a remote; before vs after first push) is encoded implicitly across a dbm-style open flag
(now mutated at runtime by the 0.9.4 fix: `'n'` downgrades to `'w'` after the replacement
push), `_written_keys`, `_deletes`, a nullable `_num_groups` resolved from
remote-metadata-or-parameter, and cached remote metadata on the session object. The
`flag='n'` semantics took three releases to pin down and produced one genuine data-loss
bug. Questions: should "replace a remote database" be an explicit operation rather than an
open flag? Should the session lifecycle be an explicit state machine? Is `num_groups`
persistence (currently: warning-only for the created-but-unpushed reopen; a sidecar file
was designed, built, then rejected for file-sprawl reasons) rightly placed?

**Seam 2 — change tracking by wall clock.** The push changelog is derived by comparing
local vs remote per-key timestamps. Consequences on record: a local edit whose timestamp
is older than the remote's silently never uploads (deferred with a warning since 0.8.4);
`set_timestamp` is user-exposed, so correctness-relevant state is user-writable; the known
"real fix" — dirty flags / a write journal in booklet — has been deferred twice. Questions:
is clock-derived change tracking defensible for this system's guarantees, or should
booklet grow modification tracking independent of timestamps? What is the minimal design
that fixes the class (not just the known instance), and what does it cost on-disk?

**Seam 3 — push atomicity and reader consistency.** A push is a multi-object sequence
(group objects → remote index → db object) with no transactional boundary. Readers are
lock-free over point-in-time index snapshots; the 0.9.3 re-check protocol handles
index-claims-vs-missing-object states but accepts a documented false-positive window
during a writer's mid-push. The group design stores byte offsets in the index that point
into group objects which are wholly rewritten on any member change — drift between the two
is handled by verification + full-object recovery fallbacks (and a deliberate silent-skip
in the recovery path). Questions: is there a design (generation-numbered index/manifest,
two-phase publish, content-addressed group objects, or similar) that removes the
inconsistency windows rather than detecting them — within the constraint of plain object
storage with NO atomic conditional writes? Is offsets-into-mutable-objects the right
coupling, or should group objects be immutable-per-generation? Where should an offline /
read-cached mode live?

## The broad mandate — the seams are a floor, not a ceiling

Beyond (and possibly instead of) the seams, sweep the whole design. Candidate angles the
author expects you to cover or consciously dismiss:

- **API surface and contracts**: the `bool | dict` tri-state `push()` return (both
  downstream layers had to be taught to check it); the `Change` object workflow; the
  exception taxonomy (`RemoteIntegrityError` arrived in 0.9.3 — is the remaining reliance
  on `ValueError`/`urllib3.HTTPError` right?); `MutableMapping` conformance including the
  RCG subclass; what a 1.0 API should freeze.
- **Concurrency model**: one exclusive S3 writer lock held for a whole session (long-lived
  remote locks; `break_other_locks` recovery); lock-free stale-snapshot readers; the
  internal index-handle swap under `_index_lock` with iteration explicitly outside the
  guarantee. Right model? Right granularity?
- **Data formats as commitments**: the remote index entry layout (fixed 15-byte
  timestamp/offset/length), the group packing format, the RCG entry schema v1, the db
  object metadata. Which of these are about to become unchangeable, and are they good
  enough to freeze?
- **Failure and operations story**: what does a user do with a corrupted or half-pushed
  remote today (is there an fsck/verify/repair story?); observability of sync decisions;
  recovery recipes currently living only in changelog prose.
- **Scale envelope**: behavior at the tethys-migration scale (many datasets × many keys;
  group sizing guidance; full-group rewrites on small updates; index download costs for
  large databases; `load_items` fan-out).
- **Security surface**: credentials handling in RCG entries and `S3Connection.to_dict()`,
  public `db_url` handling, anything the public-commons launch changes about the threat
  model.
- **booklet's role**: only where ebooklet's needs implicate it (journal/dirty flags,
  metadata slots, iteration/locking contracts) — booklet's own internals were hardened
  and reviewed this month and need no standalone re-review.

Positions the author currently holds, which you may challenge with evidence but should not
relitigate without it: local-first storage with explicit push is the right model for the
use case; the booklet→ebooklet→cfdb layering has clean responsibilities; grouped storage
answers a real S3 cost/latency problem; the recent per-fix engineering is sound — the
question is whether the seams they keep landing on need redesign, not whether the fixes
were correct.

## Constraints on viable recommendations

- Pure-Python; S3-compatible **plain** object storage only — no atomic conditional writes
  may be assumed (verified non-atomic on real providers), no server-side transactions;
  Backblaze B2 and MEGA S4 are the reference providers.
- Single-writer / many-readers per remote is the intended model.
- One maintainer; incremental migrations strongly preferred over rewrites; a sidecar-file
  aversion is on record (proposals adding per-database local files need to argue for it).
- Downstream contracts: cfdb and envlib consume `push()` returns, transparent pulls,
  `RemoteIntegrityError`, and the RCG schema. Breaking changes are possible pre-1.0 but
  each needs a migration note.
- Phasing matters more than perfection: label every recommendation **before-commons-launch
  / before-migration / post-1.0 / not-worth-it**.

## Ground rules

- Do NOT modify, create, or delete any file under `~/git/**` — all repos must be
  byte-identical when you finish (the author audits). Throwaway experiments and notes go
  under a single new directory in `/tmp/` only.
- Do not open any `*.toml` file inside any `tests/` directory (routine local config, out
  of scope). Do not make network calls to S3 services and do not run the live test tiers —
  source reading plus LOCAL experiments only (local suites and toy prototypes are fine and
  encouraged; never run two `uv` commands concurrently; use `uv run --project <repo> ...`).
- Cite everything: `file:line` for code claims, experiment output for behavioral claims,
  changelog/OPEN_WORK entries for historical claims.

## Deliverable — a single self-contained report

1. **Per-seam verdicts** for the three hypotheses: *keep / targeted refactor / redesign*,
   each with the reasoning, a concrete design sketch for any refactor/redesign (data
   structures, API shape, migration path), an honest cost estimate, and its phase label.
   If you judge a seam mis-framed, say so and re-frame it.
2. **Findings outside the seams**, severity-ranked (CRITICAL = unsound for the coming
   phase; MAJOR = will bite users or the migration; MINOR = polish), same citation
   standard.
3. **What you examined that held** — the assessed-and-sound list matters as much as the
   findings.
4. **A prioritized roadmap**: the ordered, phased list of what to change, sized roughly
   (hours/days), with dependencies between items.
5. **What you did not examine**, stated plainly.

Depth over breadth where you must choose; wrong-but-argued beats vague-but-safe. The
report is the deliverable — make it standalone readable by someone who has not seen this
brief.
