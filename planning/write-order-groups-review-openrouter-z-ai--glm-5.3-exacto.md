<!-- ai-review-harness
round:      ebooklet-wog-plan-1
agent:      openrouter
requested:  model=z-ai/glm-5.3:exacto effort=high
resolved:   model=z-ai/glm-5.3:exacto
authored-by: claude-opus-5-5
brief:      /tmp/claude-1000/-home-mike-git-envlib-repos-envlib/22497ad3-b344-4a30-8c89-5708375928be/scratchpad/brief-ebooklet-wog-plan-1.md
scopes:     ebooklet cfdb envlib envlib-ingest-wrf-3k
staging-excludes: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache
started:    2026-10-06T20:48:10+13:00
-->

<!-- staged cfdb: 1732 KiB of 5196 KiB (3464 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged ebooklet: 8184 KiB of 228148 KiB (219964 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib: 756 KiB of 1448 KiB (692 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- staged envlib-ingest-wrf-3k: 3192 KiB of 292760 KiB (289568 KiB excluded via: /.venv /node_modules __pycache__ .pytest_cache .ruff_cache .mypy_cache) -->
<!-- rebuilt from 31 agent turn(s); the report is whichever is substantive -->
I'll start by getting oriented in the repos, then build the ebooklet environment so I can run experiments against the fake S3, and read the core files in parallel.
Let me start the environment build in the background while I read the core source files.
Now let me read main.py.
Now remote.py, journal.py, and fsck.py.
Now let me check the fake S3 harness and the booklet library's `locations()` implementation, which is central to Q1.
Environment is ready. Let me examine booklet's `locations()` implementation — the foundation of the write-order claim.
I have a good picture of the core code. Let me run the baseline test suite, and in parallel examine the downstream repos (cfdb, envlib, ingest) for `num_groups` usage and the catalogue cache path.
While the suite runs, let me write the first experiment: Q1 — the write order of `locations()` across overwrites, prune, and pull-appended values.
Write order confirmed for all paths. Let me check booklet's prune implementation to understand why order is preserved, and test timestamp-eviction prune too.
Prune compacts in physical order — order preserved. Now let me check the test suite status and build experiment 2: reader-with-stale-index across a generation replace + GC (Q3), plus the delete-and-repush migration path (Q4).
The heal path works. Now experiment 3: the catalogue migration path — delete the remote from a *different* session, then push the producer file whose caches all describe the dead remote (Q4).
The migration mechanics are confirmed on current code. The full suite hit my background timeout — let me rerun it properly in the background with more time, and continue with the version-bump misclassification experiment meanwhile.
The remote was stamped 3 by the mutated code itself — I need to create it with the original code first, then reopen with the bump.
Two solid results. Now let me verify the plan's list of hash-filtering sites is complete, and reproduce the scatter assessment.
Journal v2 refusal confirmed. Now the scatter assessment reproduction:
Let me check the full test suite result and run one more small experiment on delete-only push behavior today.
The `tail` pipe buffers until completion — the suite is still running. Meanwhile, the delete-only push experiment:
Confirmed the plan's premise about delete-only pushes. Let me look at the envlib "prime" advice and `_upsert_entry` semantics the plan cites, and check the stale-incarnation test coverage.
The full suite is likely stuck on live tests timing out (no s3_config). Let me run just the hermetic fake-S3 tests in the foreground instead.
Live-S3 file, as the brief said — the hermetic core passes (83+57 tests). Let me check the hermetic pipeline test coverage for the mechanisms the plan's tail-pull reuses.
Let me check whether downstream repos import the hash functions the plan deletes, and look at the ingest config/publish flow for the migration-relevant details.
All hash-helper deletions are clean of downstream imports. Let me verify the live catalogue claim and the errors.py line refs, then check the remaining detail: whether the public catalogue HEAD still shows hash grouping.
The live catalogue HEAD confirms the plan's migration premise as of right now: `format_version: 2`, `type: RemoteConnGroup`, `num_groups: 13`. All reading is done. Before writing the final report, I'll re-run the load-bearing experiments so every experimental finding in the report carries output observed in this final pass.
`utils.py` is pristine (SUPPORTED_FORMAT_VERSION = 2). Re-running the five load-bearing experiments now for fresh verbatim output:
The journal-v2 line shows the old closed-file error — the earlier fix may not be in the saved script. Checking:
The `JournalState.load(b)` call runs after the booklet handle closes — the version check is never reached. Fixing and re-running:
Both 0.10.5 gates now show verbatim refusal messages. Re-running the last two experiments (reader heal, migration mechanics) for fresh output:
exp3's migration mechanics reproduce fully. exp2 hit leftover state from the earlier run — checking its setup:
The script doesn't clean its tmp dir, so the stale local file from the first run collided with the fresh in-memory store. Cleaning and re-running:
All experiments now carry fresh output. One last line-content confirmation for the two code sites I quote verbatim in the report:
All verification is complete. Here is the review.

---

# Review: write-order groups (format 3) replacing hash grouping

**Verdict.** The design is sound. Every mechanism the plan's answers rest on that I could execute against the current code checked out: write order is real and robust, the reader heals across generation replace + GC, the migration recipe works end-to-end at format 2, the old-client gates refuse loudly, and the motivating scatter numbers reproduce (0.3414, 2909 groups, 12.3×). I found no scenario in which the design as written loses or corrupts data. The serious findings are operational (rollout windows, the catalogue migration's failure mode) and one code-level trap the plan covers only implicitly (the unconditional format stamp). Findings are ranked most serious first; "verified" means executed or derived from code I read, with (a) what was done and (b) what was not checked.

---

## Finding 1 — Rollout ordering: between the envlib release and the catalogue migration, brand-new envlib installs hard-fail against the still-legacy catalogue

**Severity: design/rollout (availability, loud). Confidence: high (reasoned from verified gates).**

The plan releases everything in step 3 and migrates in step 4. The rationale given — "so that pip install -U envlib is the fix" — covers post-migration users but not the inverse: any user who upgrades envlib (floored to ebooklet ≥ 0.11) *after release but before Mike finishes the catalogue re-push* gets `UnsupportedFormatError` from `Catalogue()` on every use, because the catalogue is still `format_version 2, num_groups 13` (verified live this session — see Finding 10) and 0.11 refuses legacy remotes for r/w/c. `Catalogue.refresh` catches `UUIDMismatchError`/`RemoteMissingError` but propagates `UnsupportedFormatError` (read, `envlib/catalogue.py` refresh, ~795+). The migration is hours of pushing (the plan's own 420 GB estimate, not re-run here), so the window is real. The reverse order (migrate, then release) is worse: old users break with no installable fix yet. Either way a window exists; the plan should commit to running step 3 and the catalogue part of step 4 back-to-back in one session and say so. Within step 4 there is a second, smaller window: after the catalogue is format 3 but before a dataset's entry is repointed, a fresh envlib opening that not-yet-migrated member dataset sees legacy → refused; keep entry repoints immediately after member pushes and rely on the grace period before old-key deletion, as the plan already does.

- **(a)** Executed: 0.10.5's refusal gates (Finding 8/exp6) prove version gating is hard, not soft. Read: `envlib/catalogue.py` refresh exception handling; `main.py:434-537` (format gate and suppressed paths). Derived: the window arithmetic.
- **(b)** Not checked: no live test against real B2 mid-window; actual C1 timings/sizes; whether the release can be sequenced same-day in practice.

## Finding 2 — Catalogue migration failure semantics: if the re-push after `delete_remote()` dies, cold-cache users see an *empty* catalogue, not an error

**Severity: design/rollout (availability, quiet). Confidence: medium-high (reasoned + code-read).**

The catalogue step is delete_remote → push the producer file. If that push fails partway (crash, lock timeout, credentials), the remote is absent until manually re-pushed — and `Catalogue.refresh` treats `RemoteMissingError` as "treat source as empty" with a warning, so user queries silently return no datasets rather than failing. Warm caches keep serving the old catalogue (flag='r' + 404 → cached sidecar used, read `main.py` open path), so the blast radius is cold caches only, and the catalogue object itself is small (entries + metadata; the 420 GB is the members), making the gap seconds if nothing goes wrong. But the *failure* mode is silent-empty on a public service. Recommend the plan add: run delete+re-push as one script with retry, and a canary check (`Catalogue()` lists all datasets) before calling it done.

- **(a)** Read: `envlib/catalogue.py` refresh's `RemoteMissingError` handling; executed (exp3): `delete_remote()` leaves no db object (`db object in store? False`), and the subsequent producer-file push re-creates it cleanly.
- **(b)** Not checked: envlib's runtime behavior against a deleted remote was not executed; recovery-time behavior on real B2 untested.

## Finding 3 — The commit stamp is unconditional today: `format_version` must become mode-dependent or 0.11 stamps per-key remotes '3'

**Severity: implementation trap breaking a promised invariant. Confidence: high (verified).**

`utils.py:1611` writes `'format_version': str(SUPPORTED_FORMAT_VERSION)` into every commit's metadata, regardless of storage mode. The plan bumps the constant to 3 (reader cap). If the stamp keeps following the constant, every **per-key** remote pushed by 0.11 is stamped '3' — and 0.10.x clients then refuse it with "This remote database uses storage format_version 3, but this ebooklet version only supports up to 2. Upgrade ebooklet to open it." (exact message observed in exp6). That is precisely the compat break the plan promises not to make ("per-key remotes stay format 2, byte-identical"). The current code conflates two roles of one constant — reader cap and writer stamp — and the plan text never names the split. It's covered by a planned test (per-key stays 2), but make the split explicit in the plan so it survives test-list pruning: the constant becomes the cap only; the stamp is 2 for per_key, 3 for grouped.

Evidence (exp4b, executed this session against in-repo fake S3; `utils.SUPPORTED_FORMAT_VERSION` flipped to 3 at runtime, nothing else changed):

```
stamped format_version: 2 | num_groups in metadata: False     <- per-key remote, original code
grouped stamped format_version: 3 | num_groups: 5            <- after the naive bump
grouped OPENED r: k1 = b'v1'   (also c, w)
```

- **(a)** Executed: exp4b — pushed a per-key remote under original code (stamped '2', no num_groups key), then a grouped remote under the runtime bump (stamped '3', opened fine r/c/w). Read: `utils.py:1609-1613`.
- **(b)** Not checked: nothing material — this is a property of the current commit path; all 0.11 behavior is prediction.

## Finding 4 — Design economics: per-push cost is bounded by the *tail group*, not the new data — 32 MB default means a small append re-uploads up to 32 MB (and pulls it first on a non-local machine)

**Severity: design note (the plan's own benchmark will expose it). Confidence: high (arithmetic + verified semantics).**

Group objects are immutable; adding keys to the tail requires repacking and re-uploading the whole tail. Per-push overhead ≈ current tail size (0…group_bytes, mean ≈ group_bytes/2) plus the new data. With the 32 MB default: the yearly 12 GB extension pays 0.3% overhead (excellent), but a one-band append (5.7 MB — 322 chunks, measured in exp5) pays ~5.6×, and today's ~5.7 MB hash groups become 32 MB, so a scattered multi-key read inside one group over-reads first-to-last-member spans ~5.6× more bytes per ranged GET (`get_remote_group_values`, `utils.py:835-897`). The plan's step-2 benchmark measures exactly the right things ("upload bytes of a one-band append"). What the plan should state as a principle: **group_bytes must be chosen relative to typical push size and read-locality, not just object count** — 32 MB is right for yearly extensions from local-machine producers, wrong for any future frequent-small-push producer. "Keep 32 MB unless the numbers argue otherwise" is the correct posture only because the benchmark exists.

- **(a)** Derived from: immutability + repack semantics (read at `utils.py` pack/upload sites); exp5 measured one band = 322 chunks / 5.7 MB and today's 12.3× append amplification; ranged-read span semantics read at `utils.py:835-897`.
- **(b)** Not checked: actual 0.11 push byte counts and end-to-end read amplification against B2 (code doesn't exist; live untested).

## Finding 5 — Migration completeness is guarded only by migration_check; fsck and verify_remote.py cannot detect an incomplete-but-consistent migration

**Severity: design/rollout (orphan cost, not data loss). Confidence: high (reasoned).**

The migration is per-dataset new-key pushes plus catalogue entry repoints. If the session dies after a member push but before the entry repoint, the new key is an unreferenced but fully consistent orphan — invisible to fsck (nothing contradicts anything). The old entry keeps working, so nothing is lost; the cost is storage. migration_check is the only tool that proves the whole sequence complete, so it must run *after the entire step-4 session*, not per-dataset, and should also verify the catalogue entries were repointed. One discipline worth adding to the plan: freeze routine pushes to the old keys during the session. (If a routine push does land on an old key after the copy, the repoint's data push carries it — the producer file's changelog is non-empty and pushes everything; mechanism verified in exp3.)

- **(a)** Executed (exp3): the delete → producer-re-push sequence works end-to-end at format 2, including stale-incarnation reconciliation — manifest reset, remote_ts kept, journal `num_groups 13` retained, uuid match, metadata carried at its **original** timestamp, and an old reader cache that had never materialized `entry2` fetched it from the re-created remote.
- **(b)** Not checked: real multi-GB migration at B2; orphan storage cost at B2 pricing.

## Finding 6 — `copy_remote` must branch on storage_kind before the manifest, or an empty-manifest grouped remote falls into the per-key branch

**Severity: implementation edge case. Confidence: medium (code-read, not executed).**

`remote.py:470` dispatches on `if src_manifest:` — a grouped remote whose manifest is empty (all members deleted) is a reachable state (exp2's final state: `manifest after deleting both members: {}`, zero group objects) and would be routed to the per-key branch, parsing index keys as object names. Under the plan, classification must branch on storage_kind/format_version first, and the empty-manifest grouped case belongs in the test matrix. Related: `indirect_copy_remote` (`remote.py:1748-1773`) buffers a whole object via `.data` — 32 MB groups mean ~5.6× the per-object RAM of today's 5.7 MB groups; fine for B2→MEGA copies, but worth a line.

- **(a)** Read `remote.py:437-548`, `1748-1773`; executed (exp2) the delete-both state that produces an empty manifest.
- **(b)** Not checked: no execution of `copy_remote` against an empty-manifest remote — reasoned only.

## Finding 7 — Minor items

- **Ingest repo plumbing becomes a hard ValueError, and its None-guard points the wrong way.** `envlib-ingest-wrf-3k/scripts/publish.py:113-115` aborts when `entry.num_groups is None` and `:143` passes `num_groups=entry.num_groups` into `cat.publish`; config carries eleven per-dataset ints and one `None`. Under 0.11, `num_groups=int` raises at open, so the repo must be updated before any 0.11 publish run (the plan's step 4.5 does this), and the abort-if-None guard must switch to `group_bytes` or it guards a dead field. Loud failure, by design. *(Read; not executed under 0.11.)*
- **Journal v2 is a one-way door for local files.** After any 0.11 write session, the journal record's `v: 2` persists even once empty, so that local file can never again be opened by 0.10.5 ("This local file carries a journal with version 2, but this ebooklet only supports up to 1. Upgrade ebooklet to open it." — verbatim, exp6). Fine for Mike's machines; worth one line in the plan. *(Verified.)*
- **The naive bump's refusal is worse than refusal — it's destructive advice.** exp4b: under the runtime bump, the format-2 per-key remote is refused r/w/c with "This remote database uses storage format_version 2; 0.10 has no format-1 read path. Re-create the remote by re-pushing it with flag='n'…" — i.e., the v1_remote misclassification tells the user to destroy-and-recreate a perfectly good remote. Confirms the plan's storage_kind replacement is necessary, not just tidy. *(Verified.)*
- **Index bytes +27%** (15→19 per entry) on the sidecar and the db object's index section — one small object per push and local disk; negligible, and the plan states it honestly.
- **fsck's unmanifested-group detection is hash-based** (`fsck.py:128`); it disappears with the hash, and the new `empty_groups`/`bad_entry_layout` fields are the right replacements. *(Read.)*
- **Out-of-scope downstream uses I could not verify** (repos not provided): ECan `repair_precip.py:204` (`._num_groups` private access — breaks until image rebuild, as the plan says), cfdb `benchmarks/.../plan.py:8`, cfdb-ingest `forecast_archive.py:81`, ifs-download `archive.py:72`, the MEGA push scripts, `test_transfer_guards.py`. Within the three in-scope repos, my grep for `key_to_group_id` / `next_prime` / `is_prime_small` / `_num_groups` / `SUPPORTED_FORMAT` found **no** uses the plan's deletion list misses.

---

## The plan's claims that verified TRUE (Q1-Q8, with evidence)

1. **Q1 — write order.** `booklet`'s `locations()` is last-write order, period. exp1 (fresh run this session): overwrite moves the key to the end (`['a','b','d','e','c']`); `prune()` compacts in place preserving order; delete-then-re-set moves to the end; under `n_buckets=7` with 200 keys and all evens overwritten, the relocated index == last-write order; `set_timestamp` preserves order; all-overwrite == second-write order. And there is no path to a new-to-remote key without a loc_map entry — `create_changelog` drops journal keys lacking local values with a warning.
2. **Q2 — lazy deletes.** exp7 (fresh): today, a delete-only grouped push PUTs the repacked group (`new PUTs: ['testdb/4.56fe3594a8424', 'testdb']`), the deleted key leaves the committed index, the survivor stays. So lazy deletes remove real upload work, and the commit-gate extension is load-bearing: with no group uploads, `updated` is False, and the current gate (`utils.py:1556`, verbatim: `if updated or force_push or journal.meta_pending or (deletes and num_groups is None) or replace_pending or not remote_session.initialized:`) would skip the commit without the grouped-deletes term. exp2's final state (`manifest {}`, zero objects) shows the existing emptied-group GC the plan plugs into works.
3. **Q3 — gid reuse.** exp2 (fresh): a reader holding a stale manifest generation across a replace+GC heals — first fetch 404s, the re-pull refreshes index+manifest atomically, the retry succeeds, older materialized keys still read. Generations are fresh tokens and index+manifest come from one commit, so number reuse is safe for exactly the reason the plan gives.
4. **Q4 — migration.** exp3 (fresh, full output above): the delete → producer-file re-push recipe works end-to-end, including the stale-incarnation path (uuid match, remote_ts kept, journal num_groups retained, metadata re-pushed at its original timestamp) and old-reader-cache compatibility.
5. **Q5 — classification/old clients.** exp4b + exp6 (fresh): the naive bump misclassifies format-2 per-key remotes (v1_remote refusal with destructive advice) — the plan's claim is TRUE and storage_kind is the right fix; real 0.10.5 refuses format-3 remotes and journal v2 with clear upgrade messages. The per-key stamp trap is Finding 3.
6. **Q7 — motivation.** exp5 (fresh): one band = 322 chunks; the yearly append's 3542 new keys touch 2909 of G groups (0.3414); the append is 2.8% of the final dataset; upload amplification **12.3×**. Reproduced independently of the plan's numbers.
7. **Completeness of the edit list.** All 13 `key_to_group_id` sites enumerated; the plan's replacement list covers them. No in-scope downstream imports of the deleted helpers.
8. **The migration premise is live.** `curl -sI https://b2.envlib.xyz/file/envlib/envlib-commons/catalogue` (this session): `format_version: 2`, `type: RemoteConnGroup`, `num_groups: 13`, `uuid: 019f5a675c228596a89af130a2f4521a`.
9. **Current HEAD is healthy.** Hermetic test suites pass (83 + 73, run earlier this session; live-S3 files skipped per the brief).

## What was NOT checked (global, applies throughout)

- **All 0.11 behavior is prediction** — none of the code exists. Every experiment above exercised current HEAD (format 2) or 0.10.5.
- C1's 22.5× figure, the 1980s-prepend estimate, the 420 GB migration size, and per-dataset group sizing were not re-run; live B2 beyond the HEAD request above was not touched (no live push/pull/fsck — s3_config not shipped, per the brief).
- The out-of-scope files listed in Finding 7's last bullet, plus envlib docs/skills and MEGA/esa-sst repos.
- booklet versions other than the pinned 0.12.10; s3func behavior under 32 MB bodies (reliability delegated to s3func ≥ 0.9.4 per CLAUDE.md, not independently tested).

<!-- finished: 2026-10-06T21:49:46+13:00 exit=0 -->
<!-- openrouter: key string redacted 0 time(s) from the result, stream and stderr -->
<!-- openrouter (stream-visible, lower bound): 78 of 78 generations, providers={Sail Research:75,AkashML:3}, cost=$1.5934, prompt=7.50M cached=95% out=78.3k reasoning=59.0k -->
<!-- openrouter key usage: $1.6435 account-side ($35.2053 -> $36.8488; polluted by concurrent arms on the same key) -->
