# Transport review report — Claude (fresh-context subagent), 2026-07-15

Shared brief: `planning/transport-review-brief.md`. Blind held. The reviewer ran three offline loopback experiments and eight bounded live experiments against the achelous test bucket (~0.94 GB total, all objects deleted, zero-count listing verified); post-run git audit by the author confirmed zero repository modifications. Report verbatim below.

---

# Transport review: concurrent large-PUT send stalls (s3func 0.9.4 / ebooklet 0.10.1, in-tree)

## 1. Diagnosis

**The "stalls" are not stalls. They are a deterministic client-side deadline: the urllib3 *connect* timeout acts as a hard cap on the total transmission time of the entire request body, because s3func hands urllib3 the body as one `bytes` blob and CPython transmits that blob in a single SSL write call whose deadline is fixed at entry and is never extended by progress.** Any PUT whose body cannot be fully transmitted within `connect_timeout` seconds (30 s in the in-tree 0.9.4) dies with `TimeoutError('The write operation timed out')` — *regardless of whether bytes are flowing*. The network was healthy the entire time.

The five-link mechanism, each link verified:

1. **s3func passes the whole body as `bytes`** — `~/git/s3func/s3func/s3.py:279` (`body=obj` straight into `PoolManager.request`).
2. **urllib3 2.7.0 runs the entire request-send phase under the *connect* timeout.** `connectionpool.py:459`: `conn.timeout = Timeout.resolve_default_timeout(timeout_obj.connect_timeout)` before `conn.request(...)`; `connection.py:439`: `self.sock.settimeout(self.timeout)` at the top of every `request()` (fresh or reused connection). The *read* timeout is applied only afterwards, for the response (`connectionpool.py:518–530`). urllib3 has no separate send/write timeout concept.
3. **A `bytes` body is a single chunk.** `util/request.py:223–225`: `chunks = (to_bytes(body),)` — one `self.send(78MB)` → http.client `send()` → `SSLSocket.sendall(entire body)`. (File-like bodies, by contrast, are read in `blocksize=16384` chunks — `connection.py:468` — one `send()` per 16 KiB.)
4. **`SSLSocket.sendall` collapses to one `_sslobj.write(whole buffer)`.** stdlib `ssl.py`: `sendall` loops `v = self.send(byte_view[count:])`, but CPython does **not** enable `SSL_MODE_ENABLE_PARTIAL_WRITE`, so the *first* `send()` must consume the entire remaining buffer — proven empirically (offline test C below: a single `send(3 MiB)` on a throttled connection times out instead of returning a partial count).
5. **The deadline inside one `_sslobj.write()` is initialized once, at entry, from the socket timeout.** The internal `SSL_ERROR_WANT_WRITE` retry loop decrements that same deadline; successful partial flushes to the kernel do not reset it. Proven empirically (offline test A: `sendall(3 MiB)` with a 3 s timeout dies at 3.00 s while the receiver drained continuously; offline test B: the identical transfer sent 16 KiB per `send()` call completes in 35.9 s under the same 3 s timeout).

**Failure condition:** `body_bytes / per_stream_throughput > connect_timeout`. Concurrency enters *only* by dividing per-stream throughput; body size enters by multiplying transmission time. Neither ~10 streams nor ~78 MB is individually magic — the product crossed 30 s.

This retro-explains every observation in the evidence table:

- **Run C** (connect=30): every failing attempt in `e2e-run-final.log` dies on a ~30–32 s cadence per URL (30 s wall + 1-2-4 s backoff); "successful" PUTs with `put 45.2s/76.8s/109.5s/113.9s` are multi-attempt totals whose final attempt got a big enough bandwidth share (as competitors failed out) to finish inside 30 s. At threads=10 on a ~11 MB/s uplink, fair share ≈ 1.1 MB/s → a 78 MB group needs ~70 s → first attempts are *guaranteed* to fail; survival threshold is `30s × share ≈ 33 MB`.
- **Run B** (0.9.3-shaped `Timeout(total=60)`): `Timeout(60).connect_timeout` resolves to **60** (verified), so the send wall was 60 s — hence mostly the same write-timeout signature, plus `ReadTimeoutError (read timeout=8.567…)` with fractional residuals where the total budget nearly expired during send and the leftover was applied to the response read. **Run C was worse than run B because §4b tightened the send wall from 60 s to 30 s.**
- **Probes 1–2** (1×20 MB, 4×20 MB): 8–9 s transmission per stream — comfortably under any wall. Clean, as observed.
- **rclone control**: Go, its own HTTP stack, chunked writes — no monolithic-write deadline. Clean.

**Live confirmation against B2 (details in §6):** a 100 MiB single-stream PUT through unmodified `S3Session.put_object` **fails at 5.84 s with a 5 s connect wall and at 12.35 s with a 12 s wall** — the failure time tracks a client-side configuration knob exactly, with zero concurrency. The same 100 MiB body as `io.BytesIO` **succeeds through a 5 s wall** (13.66 s, 7.4 MB/s). And the full reproduction — 10×40 MiB, stock connect=30, single attempt — failed 4/10 **all at exactly 30.3 s**, while `ss -ti` sampled at 1 Hz showed every socket healthy to the last second before the abort: rto ≈ 350 ms, **retransmit backoff 0, zero retransmissions in flight**, cwnd stable, `bytes_acked` climbing every sample (the killed flows had delivered 19.5–34.6 MB of 42 MB and were progressing at 0.65–1.15 MB/s — below the 40 MiB/30 s = 1.4 MB/s survival threshold; the six survivors all reached 42.0 MB acked).

**Hypothesis verdicts:**

- **H2 — CONFIRMED, in refined form.** It is not "mis-accounted progress across SSL_want retries"; the accounting is per-`_sslobj.write()` call and the call spans the whole body. Monolithic send × fixed-deadline-per-call × connect-timeout-as-send-wall. Note the mechanism is not TLS-specific: plain-socket `sendall()` has had whole-operation deadline semantics since Python 3.5, so an `http://` endpoint would fail identically.
- **H1 — REFUTED as the mechanism.** Failures are deterministic at the wall (uniform 30.3 s in E8, 5.84/12.35 s tracking the knob), reproduce with a *single* stream and no CPU load, and the socket telemetry shows no RTO backoff, no synchronized loss collapse (steady ~1–2 % retransmit rate, normal for a saturated residential uplink). GIL/burst dynamics are real but irrelevant here.
- **H3 — REFUTED.** Same kernel, same buffers, same object shape: BytesIO body succeeds where bytes body fails; failure follows a userspace parameter.
- **H4 — REFUTED.** Same server, same key shape, same day: E5/E6/E7 differ only client-side; B2 ingested 11.36 MB/s aggregate from this client without complaint.

The open question in §3 of the brief — count vs size — is answered: **neither dominates; the product `body_size / (uplink/streams)` against the wall is the whole story.** 10×12 MiB (≈11 s/stream) was 10/10 clean; 1×100 MiB at a 5 s wall failed alone.

## 2. Findings by severity

**CRITICAL-1 — Large `bytes` bodies are transmitted under a fixed total-transmission deadline equal to the connect timeout.**
`s3func/s3.py:279` (S3 PUT), same pattern on the B2-native path (`s3func/b2.py` `put_object`, bytes POST); wall set at `s3func/http_url.py:54`; urllib3 semantics at `connectionpool.py:459`, `connection.py:439`, `util/request.py:223–225`.
Failure scenario: ebooklet push, threads=10 (S3Connection default, `ebooklet/remote.py:674`), ~10 MB/s uplink → share ≈ 1 MB/s → **every group > ~30 MB fails all 4 attempts**; each retry re-sends the full body from byte 0 into the shared link. On a 2 MB/s uplink even a *single-stream* 70 MB group fails. Consumers on slow links hit this with no concurrency at all.
Resolution: stop handing urllib3 monolithic bytes for large bodies — wrap in `io.BytesIO` (see §3). Proven live (E7).

**MAJOR-2 — Fix §4b, as shipped, made large uploads strictly worse while fixing reads.**
`Timeout(connect=30, read=timeout)` tightened the send wall from 60 s (resolved from `total=60`) to 30 s — run B: 23/23 with a retry storm; run C: 12/23 hard failures. The in-code claim at `s3func/http_url.py:48–53` ("a slow-but-progressing transfer never times out; a genuinely stalled socket still dies within `timeout`") is **true for the response/read side but false for the send side** as long as CRITICAL-1 stands. 0.9.4 must not ship without the body-chunking fix; the comment and CHANGELOG entry need correcting to describe send-phase semantics.

**MINOR-3 — Retry policy amplifies the failure mode for large bodies.**
`http_url.py:55–73`: connection-error retries re-send the entire body with backoff 1-2-4 s. With CRITICAL-1 fixed, spurious timeouts vanish and this is acceptable; for genuine transient failures on 100 MB-class bodies, consider a larger backoff factor for connection-level errors, or fewer in-flight retries. Defer.

**MINOR-4 — Push memory and fairness at high thread counts.**
`ebooklet/utils.py` phase B holds each packed group (`packed`, up to ~134 MB) in a worker for the full PUT duration; with threads=10 that is ~1.3 GB pinned (helper defaults in `remote.py:81,111` are even `threads=20` → ~2.7 GB). With the transport fixed, 10 concurrent large PUTs are *correct* but buy nothing over ~4 on this class of uplink (probe 2: 4 streams ≈ 10.2 MB/s; E8: 10 streams ≈ 11 MB/s) while inflating RTT (bufferbloat) and per-PUT wall time. An uploader gate of ~4 for bodies > 8 MiB (brief §6, remedy 1) is cheap and reasonable — as an optimization, not a correctness fix. Optional now, fine later.

**NOTE-5 — Worker-contract asymmetry.**
`ebooklet/utils.py:1304`: the group-upload `as_completed` loop calls `future.result()` unwrapped, while the pull loop and legacy per-key loop both got try/except in §4c. `upload_group` structurally cannot raise today (its whole body is inside try blocks), but a future edit to its prologue would crash the entire push again. Wrap for symmetry.

**NOTE-6 — Single-stream throughput is not a stable constant.**
Probe 1 measured 2.35 MB/s; my E2/E3/E7 measured 5.3–7.4 MB/s on the same link (window fully opened: `snd_wnd` ≈ 3.1 MB observed). Any test or sizing rule that hardcodes a rate will be wrong at some hour of the day; calibrate at runtime (see §5).

## 3. Ship recommendation

**Ship now (blocking, tiny, proven):**

1. **s3func: send large bodies as seekable streams, not monolithic bytes.** In `S3Session.put_object` (`s3.py:223`), after the existing metadata/Content-Length handling, wrap: `if isinstance(obj, (bytes, bytearray)) and len(obj) > _CHUNK_THRESHOLD: obj = io.BytesIO(obj)` (threshold ~1 MiB keeps the small-object path byte-identical). Everything downstream already works: the signer hashes file-like bodies and seeks back (`signer.py:67–77`) — or sign the bytes first and wrap after, saving one pass; `put_object` already computes Content-Length via seek/tell (`s3.py:272–276`); urllib3 streams 16 KiB per `send()` (`connection.py:468`), giving the connect timeout **per-chunk idle semantics** during upload (a genuinely stalled socket still dies in 30 s; a progressing one never does — now truly matching the §4b comment); retries rewind seekable bodies (`connectionpool.py:761–762, 884`). No urllib3 API surface changes, no Transfer-Encoding (which would break SigV4). Apply the same wrap to `b2.py put_object`. Measured cost: none — E7 (BytesIO) ran *faster* than E2 (bytes) single-stream (7.4 vs 5.3 MB/s).
2. **s3func: correct the §4b comment, docstring, and CHANGELOG** to state the send-phase semantics: read timeout = per-read idle timeout on the response; upload progress is capped per-16 KiB-chunk by the connect timeout once (1) ships.
3. **s3func: expose `connect_timeout` as a `session()`/`S3Session` parameter** (default 30). Two lines, and the deterministic regression test (§5, T2) needs it.

With (1)–(3), `threads=10` pushes of 100 MB-class groups on this link are reliable: per-stream ~1.1 MB/s never idles anywhere near 30 s per 16 KiB chunk, and each PUT simply takes the ~70–120 s it legitimately needs. **No ebooklet change is required for correctness.**

**Defer:**
- ebooklet large-body concurrency gate ≈ 4 (MINOR-4) — nice-to-have for RAM/fairness, not needed for reliability.
- Retry-backoff scaling for large bodies (MINOR-3).
- SO_SNDBUF/TCP_NOTSENT_LOWAT, Expect: 100-continue, B2 large-file API — correctly assessed in the brief as out of proportion; reject. Multipart upload remains a separate future feature for >5 GB objects, not a fix for this.
- Consider reporting the "connect timeout is the send deadline for monolithic bytes bodies" interaction upstream to urllib3; it will bite anyone doing large single-PUT uploads over slow links.

## 4. Assessment of the in-tree fixes (§4a–c)

**(a) `pack_group` bytearray — CORRECT and sufficient.** `ebooklet/utils.py:210–242`: `bytearray` append is amortized O(1); offsets arithmetic unchanged; final `bytes(buf)` restores the exact prior return contract (one extra transient copy, ~134 MB peak per packing worker, bounded by the pack gate — acceptable). The tripwire (`test_push_pipeline.py:433`, `test_pack_group_is_linear`) genuinely guards the complexity class. Side effect to note: this fix is what *unmasked* CRITICAL-1 — correct behavior, but the release notes should link the two.

**(b) timeout semantics — RIGHT DIRECTION, INSUFFICIENT ALONE, and currently a net regression for uploads (MAJOR-2).** For the read side the claim holds: the read timeout is applied per `recv` on the response (`connectionpool.py:518–530`), and response reads happen in bounded pieces, so slow-but-progressing ranged GETs are genuinely fixed — that production-relevant 0.10.0 problem is real and solved. For the send side, `connect=30` became the whole-body wall (evidence: run B vs run C; E5/E6). Ship only together with the §3 body-streaming change; fix the comment.

**(c) worker error contract — CORRECT.** All three sites route transport raises into per-group/per-key failures; run C is a live proof (12 clean `PushResult.failures`, journal intact, commit of the 11 successes succeeded, retry converges). `Exception` (not `BaseException`) is the right net — KeyboardInterrupt still propagates. The `ConcurrentCompactionError` pre-commit abort ordering (check inside the gate, after reads, before staging) is coherent. One asymmetry: NOTE-5.

## 5. Scale-test specification

Three tiers; the first two are the regression for *this* class, the third prevents silent recurrence of the run-B shape. All live tests: `@pytest.mark.scale`, bucket `achelous`, keys under a unique per-run prefix, `delete_objects(prefix=...)` + zero-count listing assertion in a `finally`, credentials via the `s3_config.toml` idiom, explicit ~500 MB/run transfer budget in the docstring.

**T1 — offline tripwire (runs in the normal suite, no credentials, no network; this is the test that would have caught the bug class years ago).** Loopback TLS sink (self-signed cert generated in tmp_path) draining at ~300 KiB/s with small SO_RCVBUF/SO_SNDBUF; drive a ~2 MiB PUT-shaped request **through s3func's real request path** (a `session()` pool pointed at `https://127.0.0.1:<port>` with `cert_reqs` relaxed, or `HttpSession` if certificate plumbing is awkward) with `connect=2, read=30`. Assert the body lands completely. Monolithic-bytes send fails this in 2 s; the chunked path passes in ~7 s. It tests the true invariant — *send progress must extend life* — against a real socket, not a mock.

**T2 — deterministic live reproduction (link-speed independent; the core regression).** Requires `connect_timeout` exposed (§3.3). Calibrate: one 32 MiB single-stream PUT → rate `r` (measured this way: 1.7–2.7 MB/s cold; do not hardcode — see NOTE-6). Then a single PUT of `S = clamp(20·r, 64 MiB, 200 MiB)` with `connect_timeout=5, max_attempts=0`. Assert: status 2xx **and** elapsed > 5 s (proves the body legitimately outlived the wall — i.e. the test really was in the failing regime; on any pre-fix stack this dies at ~5 s with `TimeoutError('The write operation timed out')`). This reproduces the exact §3 defect on any link, fast or slow, for ≤ ~150 MB.

**T3 — regime-faithful concurrency run (the run-B/run-C shape).** N=10 threads, one shared session (stock `connect=30, read=60, max_attempts=3`), body size per stream `S = clamp(30s · (r_agg/10) · 1.5, 24 MiB, 48 MiB)` where `r_agg` comes from a short 10-stream calibration burst (or reuse T2's `r` conservatively). Assert **all** of: 10/10 status 2xx; **zero `Retrying` records** captured by a `logging.Handler` attached to `urllib3.connectionpool` for the duration (this is the assertion that makes run B a *failure* instead of a pass-with-storm — the suite must treat silent full-body retries as red); aggregate throughput sanity-logged. Optionally drive the same shape through a real `ebooklet` push (12 × ~40 MiB values into a scratch db_key) asserting `PushResult.failures == {}` plus the zero-retry handler — that also pins the §4c plumbing.

Skip conditions: no credentials → skip; T3 sizes below its floor because the link is extremely fast → fall back to T2-style reduced wall (which never skips).

## 6. Experiments run

**Offline (loopback TLS, throttled sink ~320 KiB/s, 64 KiB socket buffers, python 3.12.11 / OpenSSL 3.0.16 — the e2e venv):**
- `sendall(3 MiB)`, timeout 3 s → `TimeoutError: The write operation timed out` at **3.00 s**; receiver had drained 278,528 B continuously → deadline is fixed at entry, progress does not extend it.
- Same body, 16 KiB per `send()` call, same 3 s timeout → completed 3,145,728 B in **35.88 s**, no timeout → fresh deadline per write call.
- Single `send(3 MiB)` → `TimeoutError` at **3.00 s** (no partial return) → `SSL_MODE_ENABLE_PARTIAL_WRITE` is not enabled; one `_sslobj.write()` spans the whole buffer.

**Live (bucket `achelous`, all keys `transport-review-*`, endpoint s3.us-west-002.backblazeb2.com, in-tree s3func 0.9.4 via `S3Session`; `ss -tino` sampled at 1 Hz throughout):**
- E1: 1×16 MiB bytes PUT → 10.08 s (1.67 MB/s, slow-start dominated).
- E2: 1×100.7 MB bytes PUT, stock 30 s wall, single attempt → **200 OK in 18.99 s** (5.3 MB/s — under the wall; consistent with the mechanism, which predicts failure only when transmission > wall).
- E3: same body as `io.BytesIO` → 200 OK in 14.12 s.
- E4: 10×12 MiB bytes concurrent → **10/10 OK**, aggregate 11.36 MB/s, **0 retries** → high stream count alone does not trigger the defect.
- E5: 1×100 MiB bytes, pool identical to `session()` but `connect=5`, single attempt → **failed at 5.84 s** (`ProtocolError('Connection aborted.', TimeoutError('The write operation timed out'))`).
- E6: same, `connect=12` → **failed at 12.35 s**. Failure time tracks the client-side connect knob.
- E7: same 100 MiB body as `io.BytesIO`, `connect=5` → **200 OK in 13.66 s** (7.4 MB/s) through a 5 s wall.
- E8: 10×40 MiB bytes, stock `connect=30`, single attempt each → 6/10 OK (23.97–30.47 s), **4/10 failed at 30.33 s each** (uniform). `ss -ti` in the final seconds before the aborts: all flows rto 350–357 ms, retransmit backoff 0, 0 retransmissions in flight, cwnd 32–175, `bytes_acked` increasing every 1 s sample; the killed flows had delivered 19.5–34.6 MB (rates 0.65–1.15 MB/s, below the 1.4 MB/s survival threshold), the survivors all reached 42.0 MB acked. Historical retransmits 1–2 % of bytes, RTT 145→155 ms — a healthy saturated uplink, no collapse.

Total transferred ≈ 0.94 GB across the two live runs (≈ 340 MB + ≈ 600 MB); every individual experiment ≤ 1 GB and ≤ ~90 s of full uplink, run strictly one at a time.

**Integrity statement:** No repository files were modified, created, or deleted — `git status` on `~/git/s3func`, `~/git/ebooklet`, `~/git/booklet`, and `~/git/cfdb-repos/cfdb` after the review shows exactly the pre-existing in-tree round work and nothing else; all throwaway scripts, the venv, certs, and logs live in the session scratchpad under /tmp. Credentials were loaded programmatically from `s3_config.toml` via the `test_push_integrity.py` idiom and were never printed, echoed, or written anywhere. The 2 GB reference file at `~/data/tmp_pipeline_test/sst_subset.cfdb` was not touched. All test objects were deleted after each run via `delete_objects(prefix='transport-review-', purge=True)`; the final listing under the `transport-review-` prefix returned **0 objects** (both runs).
