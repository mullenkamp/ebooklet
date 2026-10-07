# `group_bytes` benchmark (2026-10-07)

## Core

**Question:** is 32 MiB a good default for write-order groups (plan step 2)?

**Answer: yes, keep 32 MiB, and avoid 64 MiB and above.**
- Between 8 and 32 MiB every read and every push is within run-to-run noise.
- At 64 and 128 MiB a full-band read is 1.7–3× slower, and push memory rises to about 1.4 GB.

**Why the size matters for reads.** A GET streams at only about 10–12 MB/s. A full-band read issues one GET per group the band spans, so with large groups it runs on too few parallel streams:

| group_bytes | GETs | time | aggregate |
|---|---|---|---|
| 8 MiB | 18 | 1.8–2.2 s | 65–75 MB/s |
| 32 MiB | 5 | 2.3–2.5 s | ~60 MB/s |
| 128 MiB | 2 | 7.2 s | ~20 MB/s |

**Appends are cheap at every size.** A one-band append uploads 1.01–1.18× the chunks it changed. The overhead is at most about one group: the group that holds the partial band's chunks also holds the end of the previous band, and it is repacked whole.

**A trap found by the benchmark, since fixed.** `group_bytes` was then writer-side and never stored on the remote. An append that omitted it packed at the 32 MiB default, whatever size the dataset was created with.

Fixed the same day (Mike's decision): a grouped remote now records the value its last push packed with, and writers that pass none inherit it. The harness was corrected before any recorded run: its first 8 MiB remote was deleted and redone with `group_bytes` passed.

---

## Method

All scripts and raw results are in `write-order-groups-evidence/benchmark/`.

1. **Source data.** `pull.py` ran under ebooklet 0.10.5, read-only, through the public `db_url`. It pulled the last 6 complete bands (359–364) of the published `wrf-3km-nz-temperature`:
   - 1,936 keys, 856 MB, in 72 s;
   - chunks are (840, 24, 24) over a 534 × 315 grid, 322 chunks per band.
2. **Rebuild.** `rebuild.py` wrote a fresh local cfdb band by band, the way the production build writes:
   - 4 full bands plus 217 rows of the fifth, mirroring the published record, which ends 217 rows into its last band;
   - **faithfulness check:** all 1,288 full-band chunks have exactly the compressed lengths the published dataset stores for them.
3. **Initial push.** `bench.py push --phase initial` pushed a copy of that file to `scratch-bench-gb-<size>` on the member bucket, for 8, 16, 32, 64 and 128 MiB:
   - 604.5 MB of chunks per size;
   - each push ran in its own process, so peak RSS is per push;
   - PUTs were counted at the session class.
4. **Append.** `bench.py push --phase append` added exactly one band of rows through the remote handle: rows 3577–4417, which finish band 5 and then write 217 rows of band 6. It then pushed again.
   - "Changed chunks" is the sum of the new lengths of every index entry the append created or rewrote: 645 keys, 180.8 MB. That is the least any layout could upload.
5. **Reads.** `bench.py read` read credential-free through each scratch key's public `db_url`, the production read path. Each read used a fresh empty cache, and each result was checked equal to the source.
   - There were three passes over all sizes, with the size order rotated per pass.
   - Pass 1 was each object's first fetch.
6. **Cleanup.** `delete_remote()` on every scratch key, then fsck: no db object, 0 objects, 0 orphans.

## Results

### Initial push

4 bands plus 217 rows, 604.5 MB.

| group_bytes | groups | uploaded MB | push s | MB/s | peak RSS MB |
|---|---|---|---|---|---|
| 8 MiB | 75 | 605.5 | 46.9 | 12.9 | 399 |
| 16 MiB | 37 | 605.5 | 40.4 | 15.0 | 666 |
| 32 MiB | 19 | 605.5 | 37.0 | 16.3 | 961 |
| 64 MiB | 10 | 605.5 | 40.4 | 15.0 | 1,359 |
| 128 MiB | 5 | 605.5 | 38.0 | 15.9 | 1,367 |

- **Throughput** is bound by the uplink (13–16 MB/s) at every size.
- **Peak RSS** grows with the group size. Up to 32 MiB it fits about 330 MB of base plus 2 × 10 × `group_bytes`: ten upload threads, each holding a packed group, with about two copies of the bytes. At 64 and 128 MiB, 10 and 5 groups were all in flight. This model is an inference from these numbers, not traced in the code.
- The 8 MiB RSS figure comes from an identical earlier run of the same push (same bytes and groups). The re-run's line was truncated in capture.

### Append

One band of rows.

| group_bytes | changed chunks MB | uploaded MB | amplification | groups PUT | push s | peak RSS MB |
|---|---|---|---|---|---|---|
| 8 MiB | 180.8 | 185.4 | 1.03× | 10 | 19.7 | 933 |
| 16 MiB | 180.8 | 184.5 | 1.02× | 6 | 14.8 | 943 |
| 32 MiB | 180.8 | 181.8 | 1.01× | 3 | 24.8 | 886 |
| 64 MiB | 180.8 | 213.4 | 1.18× | 2 | 24.9 | 995 |
| 128 MiB | 180.8 | 212.1 | 1.17× | 2 | 20.1 | 1,018 |

- The append's RSS includes reading the new rows from the pulled source.
- **Extrapolation to production:** the overhead does not grow with the length of the record. A yearly append (about 10.4 bands, 1.5 GB) pays at most about one extra group, which is a few percent even at 128 MiB. Under hash grouping it was 10–28×.

### Reads

Through the public `db_url`, 10 threads (ebooklet's default).

| read | chunks | group_bytes | GETs | MB down | seconds (passes 1, 2, 3) |
|---|---|---|---|---|---|
| point series, all time | 6 | 8 / 16 / 32 / 64 / 128 MiB | 6 at every size | 2.5 at every size | 0.3–1.5 at every size (noise) |
| 2×2 chunks, all time | 24 | 8 MiB | 9 | 23.0 | 2.0, 1.7, 2.0 |
| | | 16 MiB | 8 | 28.6 | 1.6, 2.0, 2.1 |
| | | 32 MiB | 7 | 34.0 | 0.7, 2.0, 0.7 |
| | | 64 MiB | 7 | 34.0 | 0.7, 2.0, 2.0 |
| | | 128 MiB | 6 | 39.4 | 1.7, 2.0, 0.9 |
| one hour, full field | 322 | 8 MiB | 18 | 141.2 | 2.2, 2.0, 1.8 |
| | | 16 MiB | 10 | 141.2 | 1.9, 1.8, 2.7 |
| | | 32 MiB | 5 | 141.2 | 2.3, 2.5, 2.5 |
| | | 64 MiB | 3 | 141.2 | 3.9, 4.3, 3.9 |
| | | 128 MiB | 2 | 141.2 | 7.2, 7.2, 7.3 |

**Point series.** The time is the same at every size: one small GET per band, so it is bound by request count. The ~0.35 s and ~1.3 s runs are network noise; they appear at every size.

**2×2 region.** It needs about 10 MB of chunks. In write order, a band's two chunk rows of the region are one full chunk row (14 chunks, about 6.5 MB) apart, and one GET spans from the first wanted chunk to the last. So it over-reads 2.3–4×, and slightly less with small groups, whose boundaries split the spans. This is the "coalesce grouped ranged reads" backlog item; group size barely changes it.

**Full field.** One GET per group the band spans, so parallelism falls with the group size. 8–32 MiB is within noise; 64 and 128 MiB are clearly slower.

**First fetch versus repeats.** No consistent difference: the CDN did not visibly speed up repeated ranged GETs.

## Not measured

- Other networks and times of day. Everything ran from one home connection in NZ, uplink about 13–16 MB/s, on one afternoon.
- Machines with less memory.
- Read thread counts other than 10.
- Datasets other than temperature. Compression differs by variable (for example precipitation), but group membership depends only on bytes.
- A 35-year-long dataset. Each read touches only band-local groups, because write order keeps a band together, so the per-band costs above carry over; the point series simply scales with the number of bands.
