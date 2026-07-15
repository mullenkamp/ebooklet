# Critical review: large-body transfer behavior under concurrency (s3func + ebooklet)

**Date:** 2026-07-15 · **Author:** the maintainer of s3func/ebooklet, requesting a diagnosis-first review of his own transport stack. This brief follows a pre-release end-to-end test campaign that surfaced four previously-unknown defects in one afternoon; three are fixed in-tree, the fourth is diagnosed only to a boundary and needs a mechanism-level diagnosis plus an architecture recommendation.

## Your task

1. **Determine the mechanism** behind the per-connection send stalls described below (§3) — by source analysis AND experiment where possible. Hypotheses are listed (§5); test them, rank them, or replace them.
2. **Recommend the fix architecture**: what should change in s3func's request path and/or ebooklet's upload concurrency shape, what ships now vs later, with concrete risk assessment.
3. **Assess the three in-tree fixes** (§4) — correct, sufficient, side effects?
4. **Specify the regression test** that would have caught this class: what a `scale`-marked live test must contain to actually reproduce the failing condition (the author's constraint: it must genuinely test the issue, not a simulation that happens to pass).

Ground rules: repositories are read-only (`~/git/s3func`, `~/git/ebooklet`, `~/git/booklet`; context: `~/git/cfdb-repos/cfdb`) — no file modifications anywhere; throwaway scripts/venvs under /tmp only. Do not open `~/git/ebooklet/ebooklet/tests/s3_config.toml` in a way that displays its contents — load it programmatically (the pattern in `ebooklet/tests/test_push_integrity.py:39-49`) and never print/echo credential values. Live experiments ARE permitted and encouraged against the `achelous` TEST bucket only, under key prefixes starting `transport-review-`, bounded (≤ ~1 GB total transfer per experiment), always cleaned up afterward. The public commons and any other bucket are strictly out of scope.

## 1. Context

ebooklet pushes datasets to S3-compatible stores as ~10–150 MB "group" objects via single PUTs (SigV4, payload fully materialized and hashed upfront; s3func is the HTTP layer on urllib3). Until this round, a CPU bottleneck (§4a) meant pushes effectively ran ONE PUT at a time. The new pipelined push (ebooklet 0.10.1, in-tree) removed that bottleneck — and for the first time ever, the stack actually runs `threads=10` concurrent large PUTs. The transport layer has never been exercised in this regime, and it fails in it.

Machine: Ubuntu 24.04, NZ residential fibre; endpoint `s3.us-west-002.backblazeb2.com` (~170 ms RTT — a long fat network). Test remote: bucket `achelous`.

## 2. The evidence timeline (all logs preserved; paths in §6)

| Run | Configuration | Result |
|---|---|---|
| A | 0.10.0-shaped (pack serialized by the §4a CPU bug) | 23/23 groups, ~1.25 MB/s aggregate, no transport errors — one PUT at a time, each got the whole link |
| B | pack fixed; s3func timeout still `Timeout(total=60)` | 23/23 groups, ~4.4 MB/s, but a storm of spurious timeouts + full-body retries (each PUT had to COMPLETE in 60 s; at uplink/10 they can't) |
| C | + s3func `Timeout(connect=30, read=60)` (§4b) | **12/23 groups FAILED** — `TimeoutError('The write operation timed out')` ≈ single-socket send stalls > 30 s, exhausting 3 retries; aggregate 3.1 MB/s. 56 retry warnings. The failures land correctly in `PushResult.failures` (§4c) |
| Probe 1 | bare s3func, 1 × 20 MB PUT | 2.35 MB/s — per-stream throughput is TCP-window/RTT-limited (normal for ~170 ms RTT) |
| Probe 2 | bare s3func, 4 × 20 MB concurrent | 2.5–2.7 MB/s EACH, 10.2 MB/s aggregate, zero errors — streams scale independently; uplink ≥ 10 MB/s |
| Control | **rclone**, 10 × 78 MB concurrent SINGLE-PART PUTs (`--transfers 10 --s3-upload-cutoff 200M`), same bucket/link | **744 MB in 80 s = 9.3 MB/s, zero errors, zero retries** |

Owner's context: B2 has no account/connection upload limits, and rclone has routinely pushed large data on this link without issues.

## 3. The open defect

Ten concurrent ~78 MB single-part PUTs **through the Python stack** (http.client → urllib3 2.x → ssl, threads from a ThreadPoolExecutor) collapse: individual sockets stall > 30 s mid-body ("The write operation timed out" — the socket sits on the *connect* timeout during body transmission in urllib3), while the aggregate link is demonstrably capable of 9–10 MB/s in the identical object shape via rclone, and 4 concurrent 20 MB PUTs through the same Python stack are clean. The failing condition appears to require BOTH high stream count (~10) AND large bodies (~78 MB); which of the two dominates is not established (a 10 × 20 MB probe was not run).

## 4. In-tree fixes already made this round (review these too)

- **(a) `pack_group` quadratic bytes-concatenation** (`ebooklet/utils.py`, `bytes +=` → `bytearray`): ~100 GB of memcpy per 134 MB group, ~60–100 s of CPU — the historical reason only one PUT ever ran at a time. Fixed + complexity tripwire test.
- **(b) s3func timeout semantics** (`s3func/http_url.py:session()`): `Timeout(timeout)` set urllib3's `total=` deadline despite the parameter being documented everywhere as a READ timeout — any request slower than 60–120 s total was killed mid-body and retried from byte 0. Now `Timeout(connect=30, read=timeout)`. Note: the same wall applied to large ranged GETs for slow consumers (production-relevant on 0.10.0 today). s3func suite 82/83 green incl. live tiers.
- **(c) Worker error contract** (`ebooklet/utils.py`): transport-level raises (`MaxRetryError`) from `put_object`/pull futures crashed the whole push; now caught and routed into per-group/per-key `PushResult.failures` at all three sites (group workers, pull loop, legacy per-key loop). Verified live in run C (12 clean per-group failures, no crash, journal intact, retry converges).

## 5. Hypotheses for §3 (test, rank, or replace)

- **H1 — GIL-synchronized burst traffic → queue overflow → RTO backoff.** CPython threads sending via `sock.sendall(78MB)` on SSLSockets interleave through the GIL with heavy CPU phases (packing memcpy, SHA-256, TLS encryption). The traffic pattern becomes blast/quiet cycles; synchronized blasts overflow the bottleneck queue (residential router), tail-drop hits several streams at once, victim connections retransmit into the same bursts, exponential RTO backoff reaches tens of seconds → the observed >30 s single-socket stalls. rclone (Go scheduler, chunked writes, per-stream pacing) never synchronizes. Predicts: fewer streams and/or chunked+paced sends fix it; stalls correlate with pack/hash activity.
- **H2 — monolithic `sendall` interaction with socket timeout on SSL sockets.** http.client sends a `bytes` body as ONE `sendall()`; CPython's SSLSocket sendall loop with a timeout may mis-account progress across SSL_want retries under contention, timing out despite progress. Predicts: chunked writes with per-chunk deadlines fix it independent of pacing.
- **H3 — kernel/socket buffer sizing pathology** (e.g. autotuned SO_SNDBUF collapse under many bulk streams). Predicts: stalls reproduce with a plain-socket (non-Python?) 10×78 MB test, contradicting the rclone control — mostly refuted already, but a Python-with-small-manual-writes probe would sharpen it.
- **H4 — server-side per-connection ingest pauses at high per-client concurrency.** Weakened by the rclone control (10 concurrent single-part PUTs were clean), but rclone paces differently; a Python 10 × 20 MB probe and a Python 4 × 78 MB probe would isolate count vs size vs client.

Suggested cheap experiments (bounded, test bucket): the 10 × 20 MB and 4 × 78 MB grids through bare s3func; a variant that sends bodies in ~256 KB chunks with a generator/manual loop; stall-time correlation against concurrent CPU load (pack-like memcpy in background threads); optionally `ss -ti` snapshots during a stall to read the socket's RTO/cwnd directly.

## 6. Candidate remedies to evaluate (starting points, not conclusions)

- **Cap effective PUT concurrency for large bodies** (e.g. uploader gate ≈ 3–4 for >8 MB payloads, keep 10 for small objects): at 2.5 MB/s/stream × 4 ≈ the link's capability anyway; hugely reduces burst amplitude; trivially shippable in ebooklet. Downside: leaves per-stream RTT limits in place for very fast uplinks.
- **Chunked body transmission** in s3func (send loop over memoryview slices instead of one `sendall`; possibly with small inter-chunk yields): directly addresses H1/H2; needs care with retries (body must be re-sendable per attempt — it is, it's `bytes`) and with urllib3's API surface.
- **Retry policy for large bodies**: full-body resend ×3 with `backoff_factor=1` into an already-congested link is fuel on the fire; consider longer backoff for connection-level errors on large PUTs, or fewer in-flight retries.
- Socket options (SO_SNDBUF, TCP_NOTSENT_LOWAT), HTTP/1.1 Expect: 100-continue, or B2-native large-file API — likely out of proportion; assess only if the above fail.

## 7. Materials

- E2E logs: `~/git/ebooklet/planning/transport-evidence/e2e-run-final.log` (run C) and `e2e-run-timeout-storm.log` (run B).
- The E2E script: `~/git/ebooklet/planning/transport-evidence/e2e_sst_push_test.py` (builds a 2 GB real-cfdb subset at `~/data/tmp_pipeline_test/sst_subset.cfdb` — reusable READ-ONLY for experiments; do NOT delete or modify it).
- In-tree diffs: `git diff` in `~/git/s3func` and `~/git/ebooklet` (uncommitted round work).
- Prior round artifacts: `~/git/ebooklet/planning/push-pipeline-{design-brief,review-claude,review-gemini}.md`.

## 8. Deliverable

1. **Diagnosis**: the mechanism of §3, with the evidence that convinced you (experiments run, source lines).
2. **Findings** ordered by severity (CRITICAL/MAJOR/MINOR/NOTE), each with file:line, concrete failure scenario, suggested resolution.
3. **Ship recommendation**: the minimal change set that makes `threads=10` pushes of 100 MB-class groups reliable on a long-fat residential link, and what to defer.
4. **Assessment of fixes §4a–c.**
5. **The scale-test specification** (§ task 4).
6. **Experiments run** (what, where, measured numbers) and a statement that no repository files were modified and no credentials were displayed.
