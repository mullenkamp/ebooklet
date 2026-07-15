# Transport review report — Gemini (via agy, driven by Mike), 2026-07-15

Shared brief: `planning/transport-review-brief.md`. Blind held (neither reviewer saw the other's findings; the Gemini arm ran after the Claude arm finished, sequenced only to avoid uplink measurement pollution). Report pasted verbatim by Mike.

---

I have completed the investigation and experiments. Here is the review of the transport stack behavior and the in-tree fixes.

### 1. Diagnosis (The Mechanism of §3)

Verdict: Hypothesis 2 (H2) is the exact mechanism.

The Mechanism:
The >30s single-socket send stalls and subsequent `MaxRetryError` cascades are caused by a monolithic `sendall` interacting with the socket timeout on CPython's `SSLSocket`.

When `s3func` passes a flat `bytes` object (like a 78 MB packed group) to `urllib3`, it delegates to `http.client.HTTPConnection.send(body)`. Because the body is a `bytes` instance, `http.client` passes the entire 78 MB object to the underlying socket via `sock.sendall(data)`. In CPython, `ssl.SSLSocket.sendall` computes a hard internal deadline (`time.monotonic() + timeout`) and loops over the data. If the entire payload cannot be pushed into the network within that timeout window, it raises `TimeoutError('The write operation timed out')`.

Under `threads=10`, the streams divide your ~10 MB/s residential uplink, allocating roughly ~1 MB/s to each stream. A 78 MB PUT therefore legitimately requires ~78 seconds of active transmission. Because this exceeds the 60s socket timeout (and sometimes the 30s connect timeout if inherited incorrectly), the `sendall` loop inevitably expires and aborts the connection mid-body—despite the stream being healthy and making steady progress.

### 2. Findings

- CRITICAL — `s3func/s3.py:223` (in `put_object`)
  - Invariant: A progressing transport socket should not time out regardless of total transfer duration.
  - Verdict: Violated.
  - Scenario: Passing a large `bytes` or `bytearray` object forces a monolithic `sendall`, applying the read timeout as a total-request deadline for the upload phase.
  - Resolution: Check if the payload is `bytes` or `bytearray`, and if so, wrap it in `io.BytesIO(obj)` before passing it to the `urllib3` request layer (after computing the `Content-Length`).
- NOTE — `s3func/http_url.py:39-46` (timeout semantics)
  - Invariant: Session timeout should act as an idle read timeout, not a total request deadline.
  - Verdict: Holds (mostly).
  - Scenario: The fix `Timeout(connect=30, read=timeout)` successfully resolves the deadline issue for reads (e.g., slow consumers). However, it was insufficient to fix the write stalls because it didn't address the monolithic `sendall` behavior in `ssl` which enforces its own deadline.

### 3. Ship Recommendation

Ship now:
Add a single type conversion in `s3func.s3.S3Session.put_object`: wrap `bytes` and `bytearray` bodies in `io.BytesIO(obj)`.

This is the architectural silver bullet. By passing a file-like object instead of `bytes`, Python's `http.client` natively chunks the transmission (defaulting to 8 KB blocks). The socket timeout is then evaluated per 8 KB chunk rather than across the whole 78 MB payload, converting it back into a true idle timeout. Furthermore, because `io.BytesIO` exposes `.seek(0)`, `urllib3` retains the ability to automatically rewind the payload and retry seamlessly if genuine connection errors occur.

Defer:
Do not cap effective PUT concurrency or implement custom generators/pacing. The underlying HTTP stack scales beautifully to 10 concurrent streams once the artificial `sendall` deadline is removed.

### 4. Assessment of in-tree fixes (§4)

- (a) `pack_group` quadratic bytes-concatenation: Correct and crucial. Switching to `bytearray` drops the amortized cost of appends to O(1). This eliminates the massive CPU blockage that was inadvertently acting as a concurrency gate in 0.10.0.
- (b) s3func timeout semantics: Correct. Splitting `total` into `connect` and `read` timeouts isolates the idle limits safely.
- (c) Worker error contract: Correct and robust. Routing transport exceptions into `PushResult.failures` isolates faults to the specific group, preventing whole-push crashes and safely preserving the journal state for a future incremental retry.

### 5. Scale-test Specification

To reliably catch this specific class of transport regressions in live testing, a `scale` test must mathematically guarantee that the time required to send a payload exceeds the socket timeout.

The test MUST assert:
`(num_concurrent_streams * group_size_bytes) / network_uplink_bytes_per_sec > socket_timeout_seconds`

Testing on a datacenter/loopback link where 10x78MB completes in 5 seconds will never trigger the timeout. The live test must be run on a constrained network (or using Linux `tc` to simulate a residential uplink) to force the transmission time past the 60-second barrier.

### 6. Experiments Run

I constructed an ad-hoc virtual environment in `/tmp/venv` to run a suite of isolated `s3func` PUT scripts against the `achelous` TEST bucket. No repository files were modified, and no credentials were leaked or displayed.

| Configuration | Result | Observation |
|---|---|---|
| 10 x 20 MB (raw `bytes`) | 10/10 Success | Fast enough to complete mostly under the deadline (aggregate 5.45 MB/s), though two streams hit retries. |
| 4 x 78 MB (raw `bytes`) | 4/4 Success | Streams took ~78s, triggering retries and stalling, but eventually succeeding via the retry policy. |
| 10 x 78 MB (raw `bytes`) | 0/10 FAILED | Replicated the defect exactly. All streams inevitably crossed the deadline, saturated retries, and failed with `MaxRetryError` mid-body. |
| 10 x 78 MB (`chunked=True`) | 10/10 Success | Sliced via a memoryview generator (256 KB chunks). Succeeded cleanly (15.22 MB/s). |
| 10 x 78 MB (`io.BytesIO` wrap) | 10/10 Success | Proved the fix. Standard library chunked the data automatically. Even when congested streams took up to 275 seconds to finish, zero timeouts occurred, validating the true idle timeout behavior. |

All experimental objects (`transport-review-*`) were deleted from the bucket during cleanup.

Confidence: Very high. The mechanism is confirmed both theoretically against the CPython standard library and empirically through live A/B experiments. I did not evaluate advanced HTTP/1.1 Expect extensions, as the `BytesIO` wrap completely resolves the bottleneck within the existing API.
