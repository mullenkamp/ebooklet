# Layout for operational telemetry station data (2026-10-08)

The question: chunk shape and remote layout for continuously updated ts_ortho station datasets (the ECan
kind). New data arrives hourly per station; stations appear and disappear; a history backfill is rare;
reads are mostly at most two weeks of recent data. Decided with Mike on 2026-10-08. The decision and its
follow-ups are recorded in `envlib-ingest-base/OPEN_WORK.md`.

All simulations run the real ebooklet 0.11 push and read paths against the in-repo fake S3 (scripts
`sim*.py` here). The shape is ECan streamflow: 144 stations, hourly, about 1.3 B per stored step (plain
zstd, measured on the live remote) plus 60 B per chunk.

## Decision

- **Chunk:** one station × 2520 steps for hourly data. 2520 is highly composite and a whole number of
  days (105 days, 15 weeks). For other cadences, use the highly composite length near ~2.5 k native steps
  that is a whole number of days (15-minute data: 10,080).
- **Remote:** write-order groups with `group_bytes` set per dataset, so that one block (all stations ×
  one chunk length) is at least 2 groups. That gives about 128 KiB for streamflow-like data and 64 KiB for
  thin datasets.
- **Prerequisite:** the remote index must be pruned before each push (below).

## Why: the measurements

**Operational case** (`sim7.py`, `sim8.py`): three years of block-by-block history, averaged over 10, 50
and 90 % of the current block.

| Time chunk | Layout | Objects (3 yr) | Hourly push | 1 station, 2 weeks | All stations, 2 weeks |
|---|---|---|---|---|---|
| 168 h | per-key | 22,752 | 144 PUTs, 24 KB | 2 GETs, 0.4 KB | 288 GETs, 64 KB |
| 168 h | 64 KiB | 104 | 1 PUT, 46 KB | 2 GETs, 0.5 KB | 2 GETs, 71 KB |
| 730 h | 256 KiB | 21 | 1 PUT, 187 KB | 1 GET, 28 KB (over-read) | 1.3 GETs, 129 KB |
| 840 h | 128 KiB | 42 | 1 PUT, 157 KB | 1.3 GETs, 1.0 KB | 1.3 GETs, 147 KB |
| 1680 h | 128 KiB | 41 | 1 PUT, 223 KB | 1.3 GETs, 1.9 KB | 2.0 GETs, 278 KB |
| **2520 h** | **128 KiB** | **42** | **2 PUTs, 328 KB** | **1.3 GETs, 2.8 KB** | **3.3 GETs, 409 KB** |
| 2520 h | per-key | 1,728 | 144 PUTs, 245 KB | 1.3 GETs, 2.8 KB | 192 GETs, 405 KB |
| 8760 h | 64 KiB | 90 | 4 PUTs, 854 KB | 1 GET, 5.8 KB | 4 GETs, 831 KB |

**The index term** (estimated, not simulated). There is one index entry per chunk at about 60–70 B, and a
reader downloads the whole index on its first open after each hourly change. After 10 years at 144
stations, with the prune and a key-sized bucket count:

| Chunk | Index | All-stations 2-week read, total | Hourly push, total |
|---|---|---|---|
| 840 h | ~1.05 MB | ~1.2 MB | ~1.2 MB |
| 1680 h | ~0.53 MB | ~0.8 MB | ~0.75 MB |
| 2520 h | ~0.35 MB | ~0.76 MB | ~0.68 MB |

**Compression** (`cfdb/benchmarks/compression/results/2026-09-23_ecan_streamflow/`). shuffle+zstd-1 gives
a ratio of 3.85 at 3,125 elements and 3.79 at 1,562, against 3.91 at 25,000. So 2520 costs about 2 % in
size. Per-byte throughput is about 2.5–3× lower than at 25,000, but that is microseconds per chunk.

**Write order matters for grouped layouts** (`sim.py`, `sim2.py`). With a station-by-station layout (a
history build, or a hydrated republish), each station's current chunk sits with its own history. Every
hourly push then re-uploads nearly every group, about the whole dataset (~35 MB), until the next rollover.
Rollovers create every station's new chunk in one push, so a running dataset lays itself out block by
block. Only a large history backfill needs a staging-and-reorder step (historical blocks first, then the
current chunks together).

**Rule for exact single-station reads** (`sim2.py`, `sim4.py`): one block across all stations must be at
least ~2× `group_bytes`. When it is smaller, a group holds several blocks of every station and a
single-station read over-reads by about a group. Measured with 1-year chunks: gage height at 1 MiB groups
downloaded 4.9 MB to read 137 KB.

**The hourly push across a block's life** (`sim3.py`): 1–2 PUTs. The bytes are the current chunks (from
nearly 0 up to a full block) plus at most one group of older data shared with the rollover's tail top-up.

**Stations appearing** (`sim6.py`): a new station without history adds at most one tiny extra PUT per
hour until the next rollover. A station added with a long history (`sim5.py`) adds about one group per
hourly push until the next rollover, so many such additions at once should be written historical blocks
first.

## The remote index (measured on the live remotes, 2026-10-08)

| Remote | Live keys | Index as published | Compacted (booklet `prune()`) | Superseded entries |
|---|---|---|---|---|
| ecan-streamflow | 1,090 | 13.64 MB | 0.92 MB | 93 % |
| ecan-gage-height | 1,552 | 15.78 MB | 0.95 MB | 94 % |
| ecan-precipitation | 110 | 8.66 MB | 0.87 MB | 90 % |
| esa-sst | 127,611 | 24.94 MB | 8.89 MB | 64 % |
| wrf-3k temperature | 117,859 | 8.05 MB | 8.05 MB | 0 % |
| wrf-3k altitude | 7 | 0.86 MB | 0.86 MB | 0 % |
| commons catalogue | 16 | 0.51 MB | 0.07 MB | 86 % |

- **Superseded entries.** The index is a log-structured booklet file, and each commit ships it as is,
  superseded entries included. Every commit uploads it, and every reader re-downloads it after a change.
- **The floor.** The ~0.86 MB floor is cfdb's default of 144,013 hash buckets.

## Not measured

- Real stations' irregular reporting (the simulations rewrite every station every hour).
- The daily one-week sweep.
- Ancillary code variables, which multiply the key count.
- Sub-hourly data.
- The index rows above are estimates.
