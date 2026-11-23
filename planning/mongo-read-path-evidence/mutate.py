"""Mutation runner for the ebooklet MongoDB read path (plan Step 6).

usage: mutate.py MUTANTS.json [name ...]

MUTANTS.json is a list of {"name", "file" (relative to the ebooklet repo), "old", "new",
"tests" (pytest node ids, relative)}. For each mutant: copy the repo's `ebooklet/` package into a
fresh directory, apply the one replacement to the COPY (the old text must occur exactly once), and
run the named tests from that directory so the copy shadows the editable install. The runner
checks that the copy really is what was imported, and reports KILLED (tests failed) or SURVIVED.
Bytecode is never written (PYTHONDONTWRITEBYTECODE=1), so no stale .pyc can carry a mutant over.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

REPO = Path.home() / 'git' / 'ebooklet'
PY = REPO / '.venv' / 'bin' / 'python'


def run(mutant):
    slug = ''.join(c if c.isalnum() else '-' for c in mutant['name'])[:40]
    base = Path(__file__).resolve().parent / 'mut'
    base.mkdir(exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix=f'{slug}-', dir=base))
    shutil.copytree(REPO / 'ebooklet', work / 'ebooklet', ignore=shutil.ignore_patterns('__pycache__'))
    target = work / mutant['file']
    text = target.read_text()
    n = text.count(mutant['old'])
    if n != 1:
        shutil.rmtree(work)
        return f'BAD MUTANT (old text found {n} times)'
    target.write_text(text.replace(mutant['old'], mutant['new']))
    env = dict(os.environ, PYTHONDONTWRITEBYTECODE='1')
    check = subprocess.run([str(PY), '-c', 'import ebooklet, sys; print(ebooklet.__file__)'],
                           cwd=work, env=env, capture_output=True, text=True)
    if not check.stdout.strip().startswith(str(work)):
        shutil.rmtree(work)
        return f'SHADOWING FAILED (imported {check.stdout.strip()})'
    res = subprocess.run([str(PY), '-m', 'pytest', '-q', '-p', 'no:cacheprovider', '-x', *mutant['tests']],
                         cwd=work, env=env, capture_output=True, text=True, timeout=300)
    tail = [line for line in res.stdout.splitlines() if line.strip()][-1:] or ['']
    shutil.rmtree(work)
    return ('KILLED' if res.returncode != 0 else 'SURVIVED') + f'  ({tail[0][:110]})'


def main():
    mutants = json.loads(Path(sys.argv[1]).read_text())
    only = set(sys.argv[2:])
    worst = 0
    for m in mutants:
        if only and m['name'] not in only:
            continue
        verdict = run(m)
        print(f'{m["name"]:40s} {verdict}')
        if not verdict.startswith('KILLED'):
            worst = 1
    sys.exit(worst)


if __name__ == '__main__':
    main()
