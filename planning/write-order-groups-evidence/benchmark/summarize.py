"""Tables from pushes8.jsonl + pushes.jsonl + reads.jsonl."""
import json
import pathlib
import statistics

HERE = pathlib.Path(__file__).resolve().parent
rows = [json.loads(line) for f in ('pushes8.jsonl', 'pushes.jsonl') for line in (HERE / f).read_text().splitlines()
        if line.startswith('{')]
push = {(r['size'], r['phase']): r for r in rows if r.get('cmd') == 'push'}
sizes = sorted({s for s, _ in push})

print('## Initial push (4 bands + 217 rows, 604.5 MB of chunks)\n')
print('| group_bytes | groups | uploaded MB | push s | MB/s | peak RSS MB |')
print('|---|---|---|---|---|---|')
for s in sizes:
    r = push[(s, 'initial')]
    mb = (r['group_put_bytes'] + r['db_object_bytes']) / 1e6
    print(f"| {s} MiB | {r['n_groups']} | {mb:.1f} | {r['push_s']:.1f} | {mb / r['push_s']:.1f} | {r['peak_rss_mb']} |")

print('\n## Append (one band of rows: finish band 5, 217 rows of band 6)\n')
print('| group_bytes | changed chunks MB | uploaded MB | amplification | groups PUT | push s | peak RSS MB |')
print('|---|---|---|---|---|---|---|')
for s in sizes:
    r = push.get((s, 'append'))
    if not r:
        continue
    up = r['group_put_bytes'] / 1e6
    ch = r['changed_bytes'] / 1e6
    print(f"| {s} MiB | {ch:.1f} | {up:.1f} | {up / ch:.2f}x | {r['group_puts']} | {r['push_s']:.1f} | {r['peak_rss_mb']} |")

p = HERE / 'reads.jsonl'
if p.exists():
    reads = [json.loads(line) for line in p.read_text().splitlines() if line.startswith('{"cmd"')]
    for name in ('point_series', 'region_2x2', 'field_1h'):
        print(f'\n## Read: {name}\n')
        print('| group_bytes | GETs | MB downloaded | pass 1 s | passes 2-3 s (median) |')
        print('|---|---|---|---|---|')
        for s in sizes:
            rs = sorted((r for r in reads if r['size'] == s), key=lambda r: r['rep'])
            if not rs:
                continue
            first = [r[name]['read_s'] for r in rs if r['rep'] == 1]
            later = [r[name]['read_s'] for r in rs if r['rep'] > 1]
            g = rs[0][name]
            print(f"| {s} MiB | {g['gets']} | {g['mb']:.1f} | {first[0] if first else '-'} | "
                  f"{statistics.median(later) if later else '-'} |")
