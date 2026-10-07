"""Step 2 (local only): write a fresh cfdb from the pulled bands, band by band, as the production build does:
4 full bands plus 217 rows of the fifth (the published record ends 217 rows into its last band).

Then check the rebuild is faithful: every full-band chunk must have exactly the compressed length the published
dataset stores for it (same encoding, same compression)."""
import json
import pathlib
import sys

import booklet
import cfdb

HERE = pathlib.Path(__file__).resolve().parent
BAND = 840
R0 = BAND * 359                    # first pulled row (band 359 of the record)
PARTIAL = 217                      # rows of the last band the record holds
SEGMENTS = [(0, 840), (840, 1680), (1680, 2520), (2520, 3360), (3360, 3360 + PARTIAL)]
YCHUNK = 24


def write_rows(dst, src, r0, r1):
    """Append time rows [r0, r1) (relative to R0) and write their data one chunk-row of y at a time: 25 MB per
    write, the same chunk order a whole-band write produces."""
    if r0 > 0:
        dst['time'].append(src['time'][R0 + r0:R0 + r1].data)
    ny = src['y'].shape[0]
    for y0 in range(0, ny, YCHUNK):
        y1 = min(y0 + YCHUNK, ny)
        dst['temperature'][r0:r1, y0:y1, :] = src['temperature'][R0 + r0:R0 + r1, y0:y1, :].data


def main(out):
    with cfdb.open_dataset(HERE / 'pull.cfdb', allow_partial=True) as src, \
            cfdb.open_dataset(out, 'n', dataset_type='grid', compression=src.compression,
                              compression_level=src.compression_level) as dst:
        dst.create.coord.like('y', src['y'], copy_data=True)
        dst.create.coord.like('x', src['x'], copy_data=True)
        ## coord.like() without data rejects a datetime coordinate's int step (cfdb 0.10/0.11), so the time
        ## coordinate is created with the first band's values (the data path) and appended from there.
        t = src['time']
        dst.create.coord.generic('time', t[R0:R0 + SEGMENTS[0][1]].data, dtype=t.dtype,
                                 chunk_shape=t.chunk_shape, step=t.step, axis=t.axis)
        dst.create.data_var.like('temperature', src['temperature'])
        dst.attrs.update(dict(src.attrs.data))
        dst['temperature'].attrs.update(dict(src['temperature'].attrs.data))
        for r0, r1 in SEGMENTS:
            write_rows(dst, src, r0, r1)
            print(f'wrote rows {r0}..{r1}', flush=True)

    ## Faithfulness: full-band chunks must match the published compressed lengths exactly.
    source = json.loads((HERE / 'source_lengths.json').read_text())
    checked = 0
    with booklet.open(out) as b:
        for key, _ts, _off, ln in b.locations():
            name, _, pos = key.partition('!')
            if name != 'temperature':
                continue
            t0, y0, x0 = (int(p) for p in pos.split('.'))
            if t0 >= 3360:
                continue                       # the partial band: 217 rows, not comparable
            src_key = f'temperature!{t0 + R0}.{y0}.{x0}'
            assert source[src_key] == ln, (key, source[src_key], ln)
            checked += 1
    assert checked == 4 * 322, checked
    print(f'faithful: {checked} full-band chunks have the published compressed lengths')


if __name__ == '__main__':
    main(pathlib.Path(sys.argv[1]))
