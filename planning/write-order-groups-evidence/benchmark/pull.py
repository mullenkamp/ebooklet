"""Step 1 (ebooklet 0.10.5, read-only): materialize 6 complete bands of the published WRF-3k temperature
dataset, plus its coordinates, into a local cache through the public db_url. Run with the wrf-3k repo's .venv.

Also records each pulled chunk's compressed length from the remote index, so the rebuild can be checked
against it (same encoding and compression -> same lengths)."""
import json
import pathlib
import time

import cfdb
import ebooklet
from ebooklet import utils

assert ebooklet.__version__ == '0.10.5', ebooklet.__version__
URL = 'https://b2.envlib.xyz/file/envlib/envlib/wrf-3km-nz-temperature'
HERE = pathlib.Path(__file__).resolve().parent
BAND = 840
BANDS = list(range(359, 365))      # the last 6 complete bands (band 365 holds 217 rows)

ds = cfdb.open_edataset(URL, HERE / 'pull.cfdb')
blt = ds._blt
idx = blt._remote_index
starts = {BAND * b for b in BANDS}
want, lengths = [], {}
for key, val in idx.items():
    name, _, pos = key.partition('!')
    if name == 'temperature':
        if int(pos.split('.')[0]) in starts:
            want.append(key)
            lengths[key] = utils.bytes_to_int(val[11:15])
    elif name in ('time', 'x', 'y'):
        want.append(key)
assert len(lengths) == 322 * len(BANDS), len(lengths)
print(f'pulling {len(want)} keys ({sum(lengths.values()) / 1e6:.0f} MB of temperature chunks)')
t0 = time.time()
failures = blt.load_items(want)
assert not failures, failures
print(f'pulled in {time.time() - t0:.0f}s')
(HERE / 'source_lengths.json').write_text(json.dumps(lengths))
ds.close()
