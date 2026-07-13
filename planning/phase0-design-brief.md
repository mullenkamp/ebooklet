# Critical design review: ebooklet 0.9.5 (Phase 0 hardening round) — pre-implementation

(Shared reviewer brief, 2026-07-12. Two independent reviewers receive this identical brief and work blind to each other. The BRIEF section below is verbatim-identical between them; only the per-session run instructions differ.)

## Run instructions for this session

- Work in a single scratch directory of your own under `/tmp` (e.g. `/tmp/phase0-review-gemini/`). You may write and run throwaway scripts/experiments there where they settle questions — encouraged.
- Do NOT modify, create, or delete any file under `~/git/` — all repos must be byte-identical when you finish (the author audits with git status).
- Do not open any `*.toml` file inside any `tests/` directory (routine git-ignored local config, out of scope — don't open).
- No network calls to S3 services; no live test tiers. Source reading + LOCAL experiments only. To run code against a repo use `uv run --project <repo> python ...` from your scratch dir; never run two uv commands concurrently.
- Deliver the report in the chat when done.

=== BRIEF ===

You are reviewing the DESIGN of a small hardening release before it is implemented. Verify every claim against the real source, probe for failure modes, and challenge the design's assumptions wherever the evidence points elsewhere — a report that only agrees is a failed report. The roadmap (which issues to fix) was settled by a prior two-reviewer architecture assessment; your job is to stress-test HOW this round proposes to fix them, though you may also challenge the WHAT with evidence.

## System context

Stack (same author, pure Python): booklet 0.12.6 (`~/git/booklet`) — single-file KV store; ebooklet 0.9.4 (`~/git/ebooklet`, branch dev) — THE SUBJECT, a booklet synced to S3-compatible object storage via s3func 0.9.2 (`~/git/s3func`); consumers: cfdb 0.9.1 (`~/git/cfdb-repos/cfdb`) and envlib (`~/git/envlib-repos/envlib`, 0.1.1 staged). Local-first reads/writes; explicit push(); optional grouped storage (num_groups: keys hash into N packed objects named by decimal group id, ranged reads per member). Remote namespace for a database at key D: the db object at exact key `D` (body = the serialized remote-index booklet; S3 metadata = timestamp/uuid/type/init_bytes[/num_groups], written at utils.py:723-730, read back at remote.py:180-199); child objects at `D/<decimal-group-id | user-key | _metadata>`; lock tickets at `D.lock.<13hex>-<seq>` (s3func locking.py:166,262); plus a transient writability-probe object currently at `D<13hex>` with NO separator (remote.py:311).

Constraints on viable designs: plain object storage only (no atomic conditional writes — verified non-atomic on real providers); single-writer/many-readers per remote; one maintainer, minimal diffs preferred; NO sidecar files; the author has ruled that NO old-format/backward compatibility is required (few remotes exist, all his — but the public-commons launch will freeze surfaces soon after this round). Push is always explicit; nothing pushes on close. Background docs in-repo if wanted: architecture-assessment-synthesis.md (the adopted roadmap), architecture-assessment-task.md, the two assessment reports, and CHANGELOG.md 0.8.4→0.9.4 (dense failure history).

## The six proposed changes

### 1 (CRITICAL fix, ships first) — exact-key deletes replacing prefix deletes

Today `S3SessionWriter.delete_object(key)` (remote.py:346-354) executes `self._write_session.delete_objects(prefix=self.write_db_key + '/' + key)` — a string-PREFIX delete for what must be a single-object delete. Its one production caller is the emptied-group delete in upload_group (utils.py:307), passing `str(group_id)` (non-zero-padded decimal). Consequence: with num_groups=13, emptying group 1 also destroys live groups 10 and 12 (demonstrated end-to-end in the assessment). Similarly `delete_remote()` (remote.py:367-376) deletes `prefix=self.write_db_key` with no separator: wiping `mydb` also wipes a sibling database `mydb2` — and every flag='n' push calls delete_remote() (utils.py:530).

Proposed:

(a) `delete_object(key)` → `self._write_session.delete_objects([self.write_db_key + '/' + key])` — s3func's exact-keys path with purge=True resolves version ids per key via list_object_versions and FILTERS the listing to the exact key (s3func s3.py:423-460, filter at :441,:451), i.e. exact-key all-versions delete. Contract change: catch urllib3 HTTPError and return error-or-None (mirroring upload_value, utils.py:511-521); upload_group's empty-group branch checks it and returns (error, None) on failure → the push failures dict → that group's deletes are retained (utils.py:666-675) → next push retries. Note a failed empty-group delete leaves only an orphan invisible to readers (the pushed index no longer references the group).

(b) `delete_remote()` → bounded two-step: FIRST exact `delete_objects(keys=[self.write_db_key])` (db object = existence marker; readers then see clean absence), THEN `delete_objects(prefix=self.write_db_key + '/')` (children). A torn delete leaves only invisible orphans, swept by the natural rerun (repeated delete_remote, or the still-flag='n' re-push). Deliberately NEVER deletes the `D.lock.` namespace: today's bare-prefix delete removes the calling writer's OWN live lock tickets mid-'n'-push (the session holds its lock through update_remote), leaving the writer unprotected — and removing OTHER writers' tickets is the known force_lock hazard (Phase 1 adds age-gated breaking). Keep the `_init_bytes = None; uuid = None` reset.

(c) The writability probe (remote.py:303-320) relocates its test key INSIDE the namespace: `self.write_db_key + '/_writable_probe_' + uuid.uuid6().hex[-13:]` (reserved-name precedent: `_metadata`; the probe already deletes by exact key + version_id, so even a pathological collision with a user key is non-destructive on versioned stores). Pre-existing probe orphans at `D<13hex>` are indistinguishable-by-prefix from sibling dbs and are accepted as unsweepable.

Questions to probe: is the exact-keys purge path correct on providers whose list_object_versions is eventually consistent (delete during concurrent overwrite)? Is db-object-first the right delete order for delete_remote (vs children-first), considering readers' _resolve_missing re-check and the flag='n' rerun path? Is excluding the lock namespace from delete_remote correct in BOTH contexts (mid-'n'-push under own lock; user-invoked teardown)? Any caller of delete_object/delete_remote the design missed (grep)? Does the probe relocation break anything that lists or parses child keys (copy_remote's blind listing at remote.py:400-409; group-id parsing anywhere)? Is returning error-or-None from delete_object the right failure contract vs raising?

### 2 — deleted-member recovery routed through the 0.9.3 re-check protocol

Today a member ABSENT from a group object that returned HTTP 200 (index claims it; ranged-read verification fails; full-object recovery runs) is silently not materialized (recover_group_members, utils.py:316-338, skip at :327-331): `key in db` says True (index-based, main.py:464-468) while `db[key]` raises KeyError (main.py:470-478,737-743) — two consistency machineries selected by the accident of group co-membership, since a whole-object 404 gets the full re-check-then-loud protocol (_resolve_missing, main.py:652-708). Meanwhile the PUSH path deliberately self-heals the same state: members with no local value after the pull are dropped from the index and the group repacked without them (lost-keys, utils.py:605-623) — that self-heal is the documented recovery mechanism.

Proposed: recover_group_members + get_remote_group_values (utils.py:316-401) gain `report_missing_members: bool = True`. Read paths keep the default: recovered members are materialized as today, then a MissingRemoteObject(str(group_id), missing_members) marker is returned instead of silent success — the existing marker plumbing in _load_item (main.py:711-734) and load_items (main.py:586-597) already routes it into _resolve_missing (ONE index re-pull: fresh index no longer claims → clean absence + stale-local purge; still claims → RemoteIntegrityError). The push-path call site (utils.py:595) passes False, preserving the self-heal. Log/repr wording adjusted so the member case doesn't claim the whole object was deleted.

Questions to probe: is a bool parameter the right seam (vs a marker subclass, vs handling at the update_remote call site)? Does the marker's semantics hold when SOME members recover and some don't (partial materialization then loud failure for the rest — is that ordering safe for load_items' batching)? Any OTHER caller of get_remote_group_values/recover_group_members? Does _resolve_missing's clean-absence branch behave correctly when the s3_key (a group id) still exists as an object? Interaction with the accepted mid-push false-positive window?

### 3 — format_version headroom

Writer adds 'format_version': '1' to the db-object S3 metadata dict (utils.py:723-730). Reader (_load_db_metadata, remote.py:180-199) sets self.format_version = int(meta.get('format_version', 1)) (absence = v1; None on 404) and RAISES loudly when the value exceeds what this version supports ("this remote requires a newer ebooklet") — the payoff that lets the Phase-1 storage redesign bump to v2 cleanly under the no-compat ruling. Also: document the fixed 15-byte remote-index entry (timestamp 7 + offset 4 + length 4; length==0 ⇒ per-key/metadata entry, >0 ⇒ grouped — the discriminator already used at utils.py:587) in code comments and the README formats section (item 5).

Questions: right exception type for the too-new raise (ValueError vs RemoteIntegrityError vs new)? Where exactly should the guard fire so both readers and writers refuse (reader init vs every metadata load)? Does any code path construct a reader without _load_db_metadata?

### 4 — close-time warning on pending deletions

`_deletes` (main.py:331) is memory-only; deletions not pushed before close are silently lost (and can resurrect). Interim (the Phase-1 journal is the real fix): in close() (main.py:781-786), before the finalizer: if self.writable and self._deletes → warnings.warn(UserWarning) naming the count and remedy, matching the house style of the num_groups warning (main.py:293-299, explicit stacklevel). close() only — sync() stays silent (it's a local-only op).

Questions: close() is also reached via __exit__ and the weakref finalizer — will the warning fire usefully (or double-fire / fire at GC)? Should a failed-push-then-close still warn (retained_deletes)? Is UserWarning the right channel vs logger.warning?

### 5 — declare formats frozen (docs)

New README section (e.g. "Data formats and stability") documenting: the db object (body + metadata incl. format_version v1), the 15-byte index entry + length discriminator, the group pack layout ([key_len:>H][key][ts:7][value_len:>I][value], utils.py:311-313), and the RCG entry schema v1 DECLARED FROZEN (fields per the add docstring, main.py:890-898 — also tighten "stable contract" to "frozen"). Probe: is anything in these formats NOT good enough to freeze given the known Phase-1 redesign (generational group objects, which will bump format_version)?

### 6 — warn on non-HTTPS db_url

In S3Connection.__init__ (remote.py:600 area): if db_url is set and its scheme is http → UserWarning (public consumers fetch over plaintext). The bare-URL path (check_remote_conn, remote.py:41) constructs S3Connection, so one choke point should cover it — verify; if create_s3_read_session (remote.py:85-89) can receive a raw string bypassing it, warn there too. Probe: right place, right channel, any legitimate http:// use case (local minio testing?) that needs an escape hatch?

## Verification plan to assess

New hermetic test layer: ebooklet/tests/fake_s3.py adapted from the committed architecture-assessment-harness-fake_s3.py (an in-memory fake of the s3func session API that faithfully reproduces string-prefix delete semantics), driving credential-free regression tests that FAIL on 0.9.4: group-1-vs-10/12 survival; sibling-db (mydb/mydb2) survival + lock-ticket survival through delete_remote; probe-namespace assertion; the member-integrity tests for item 2 (loud on read, self-heal preserved on push); warning tests; format_version tests. Plus one LIVE grouped test (B2) asserting sibling group objects survive an emptied-group delete via real listing (real version semantics). Pre-fix failure proof via a git worktree at HEAD. Probe: does the fake's fidelity actually cover the failure modes (it models one version per key — what does that miss?), and are there missing regression cases?

## Deliverable

A single self-contained report: (1) verdict per item — implementable as specified / needs change (with the concrete change) / blocking; (2) findings ranked CRITICAL/MAJOR/MINOR with file:line citations and, where behavioral, experiment evidence; (3) what you checked that held; (4) anything adjacent the round should absorb or explicitly defer. Depth over breadth; wrong-but-argued beats vague-but-safe.

=== END BRIEF ===
