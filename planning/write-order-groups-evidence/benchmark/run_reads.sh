#!/bin/bash
# Three read passes over every size, rotating the size order per pass. Pass 1 is each object's first fetch
# through the CDN (nothing has read these scratch objects before); passes 2 and 3 are repeats.
cd "$(dirname "$0")"
PY=../stackenv312/bin/python
ORDERS=("8 16 32 64 128" "128 64 32 16 8" "32 8 128 16 64")
for rep in 1 2 3; do
  for s in ${ORDERS[$((rep - 1))]}; do
    timeout 1200 $PY bench.py read --size "$s" --rep "$rep" 2>&1 | grep '^{' >> reads.jsonl || echo "{\"FAIL\": \"$s $rep\"}" >> reads.jsonl
  done
done
