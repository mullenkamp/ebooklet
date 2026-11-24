# bench_mongo.py: MongoDB 8.0.38 against 9.0.8 (2026-11-24)

Plan "EDR plan 2", step 0. The same ebooklet code (0.13.1 working tree) ran against two throwaway
`mongod`s on tmpfs on one machine. There were three passes of each, alternating 8.0, 9.0, 8.0 and so
on, with nothing else heavy running.

`ab_summary.txt` gives the median of the three per-pass medians, 8.0's run-to-run spread
((max − min) / median), and the 9.0/8.0 ratio. Below 1 means 9.0 is faster.

- **Single-key round trips are faster on 9.0:** `mongo_get_warm`, `mongo_get_cold` and
  `mongo_get_miss` are 0.81–0.86, against a 2–7 % spread.
- **Refreshes are faster too:** 0.86–0.95.
- **Batched reads are within the noise:** `mongo_load_*` and `store_open`.
- **The controls, which make no MongoDB call** (`booklet_*`, `ebooklet_warm_read`), stayed at
  0.99–1.01, so the machine was steady.

Verdict: no regression on 9.0.
