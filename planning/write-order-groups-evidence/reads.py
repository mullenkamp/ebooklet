from ebooklet.utils import key_to_group_id
from cfdb.utils import make_var_chunk_key
G = 8521
sp = [(y, x) for y in range(0, 315, 24) for x in range(0, 534, 24)]
bands = range(0, 306_817, 840)
pats = {
    'whole field, one band (322 keys)': [make_var_chunk_key('temperature', (100 * 840, y, x)) for y, x in sp],
    'point series, full record (366 keys)': [make_var_chunk_key('temperature', (t, 120, 240)) for t in bands],
    '2x2 tiles, 1 year (11 bands, 44 keys)': [make_var_chunk_key('temperature', (t, y, x)) for t in range(0, 11 * 840, 840) for y in (120, 144) for x in (240, 264)],
    'whole dataset': [make_var_chunk_key('temperature', (t, y, x)) for t in bands for y, x in sp],
}
for k, keys in pats.items():
    print(f'{k:<40} keys {len(keys):>7,}  GETs grouped {len({key_to_group_id(q, G) for q in keys}):>6,}  per-key {len(keys):>7,}')
