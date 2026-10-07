"""Groups touched by an append/prepend under ebooklet's hash grouping, over real cfdb chunk-key strings."""
import math
import sys

from ebooklet.utils import key_to_group_id
from cfdb.utils import make_var_chunk_key


def keys_for(var, t_range, t_chunk, spatial):
    """cfdb chunk keys for time steps [t0, t1) (absolute storage positions) and the spatial chunk starts."""
    t0, t1 = t_range
    first = (t0 // t_chunk) * t_chunk
    out = []
    for ts in range(first, t1, t_chunk):
        for sp in spatial:
            out.append(make_var_chunk_key(var, (ts, *sp)))
    return out


def spatial_starts(shape, chunk):
    grids = [[0]]
    for n, c in zip(shape, chunk):
        grids = [g + [s] for g in grids for s in range(0, n, c)]
    return [tuple(g[1:]) for g in grids]


def report(label, var, G, n_t, t_chunk, sp_shape, sp_chunk, events):
    sp = spatial_starts(sp_shape, sp_chunk)
    all_keys = keys_for(var, (0, n_t), t_chunk, sp)
    n = len(all_keys)
    gid = {k: key_to_group_id(k, G) for k in all_keys}
    sizes = {}
    for g in gid.values():
        sizes[g] = sizes.get(g, 0) + 1
    print(f'\n## {label}: G={G}, {n:,} chunk keys, {n/G:.1f} keys/group (max {max(sizes.values())}), '
          f'{len(sp)} spatial chunks per time band')
    for ev_label, (a, b) in events:
        ev_keys = keys_for(var, (a, b), t_chunk, sp)
        touched = {key_to_group_id(k, G) for k in ev_keys}
        # bytes: groups are uniform in expectation; the final dataset includes the new keys
        existing = set(all_keys)
        new_keys = [k for k in ev_keys if k not in existing]
        final_n = n + len(new_keys)
        up_frac_of_final = sum(sizes.get(g, 0) for g in touched) / final_n + len(new_keys) / final_n
        # (members already present in touched groups) + (the new members) over the final key count
        up_frac_of_final = (sum(sizes.get(g, 0) for g in touched) + len(new_keys)) / final_n
        payload = len(ev_keys) / final_n
        print(f'  {ev_label:<34} keys {len(ev_keys):>7,}  groups touched {len(touched):>6,} '
              f'({100*len(touched)/G:5.1f} %; Poisson {100*(1-math.exp(-len(ev_keys)/G)):5.1f} %)  '
              f'upload {100*up_frac_of_final:5.1f} % of final  amplification {up_frac_of_final/payload:5.1f}x')


HOURLY_T = 306_817                      # 1990-07-01T00 -> 2025-07-01T00
BAND = 840
YEAR_H = 8760
PRE_H = 92_016                          # 1980-01-01 -> 1990-07-01
sp_shape, sp_chunk = (315, 534), (24, 24)

hourly_events = [
    ('one 840-h band append', (HOURLY_T, HOURLY_T + BAND)),
    ('one year append', (HOURLY_T, HOURLY_T + YEAR_H)),
    ('1980s prepend (negative starts)', (-PRE_H, 0)),
]
for name, G in [('precipitation', 1999), ('temperature', 8521), ('barometric_pressure', 11197),
                ('evapotranspiration', 4523)]:
    report(f'wrf-3k {name}', name, G, HOURLY_T, BAND, sp_shape, sp_chunk, hourly_events)

DAILY_T = 12_785
report('wrf-3k snow_water_equivalent', 'snow_water_equivalent', 23, DAILY_T, 360, sp_shape, sp_chunk,
       [('one year append', (DAILY_T, DAILY_T + 365))])

# C1 (review c1-store-plan-1): 45 yr, 3-hourly, 324 x 277, chunks (24, 60, 60)
STEPS_YR = 2920
for G in (1009, 5003, 20011):
    report('C1 variable', 'precip_tr', G, 45 * STEPS_YR, 24, (324, 277), (60, 60),
           [('one sim-year (the 45th)', (44 * STEPS_YR, 45 * STEPS_YR))])
    # cumulative upload over 45 yearly pushes, in final-file units
    sp = spatial_starts((324, 277), (60, 60))
    present = {}
    total_up = 0
    for y in range(45):
        ev = keys_for('precip_tr', (y * STEPS_YR, (y + 1) * STEPS_YR), 24, sp)
        for k in ev:
            g = key_to_group_id(k, G)
            present.setdefault(g, set()).add(k)
        touched = {key_to_group_id(k, G) for k in ev}
        total_up += sum(len(present[g]) for g in touched)
    final = sum(len(s) for s in present.values())
    print(f'  45 yearly pushes upload {total_up/final:.1f}x the final file (G={G})')
