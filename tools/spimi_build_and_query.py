"""CLI helper to build a SPIMI index for a table and run queries.

Usage:
  python tools/spimi_build_and_query.py build <table_dir> <column> <index_out_dir>
  python tools/spimi_build_and_query.py query <index_out_dir> "some query" [k]

This script reads the table's `data.dat` by using the existing DataFile API
to iterate records. To keep this file independent we will require the user
to pass a simple JSONL-like listing or rely on the in-repo DataFile if available.
"""
from __future__ import annotations

import sys
import os
import json
from typing import Tuple

from indexes.spimi import build_spimi_blocks, merge_blocks, search_topk


def usage():
    print(__doc__)


def build_command(table_dir: str, column: str, index_out_dir: str):
    # Expect that caller has already produced a `docs.json` file with array of {"rid": [p,s], "text": "..."}
    docs_json = os.path.join(table_dir, 'spimi_docs.json')
    if not os.path.exists(docs_json):
        print(f"Expected {docs_json} with docs to index. Create it from your table data first.")
        return

    with open(docs_json, 'r', encoding='utf-8') as f:
        docs = json.load(f)

    def iter_docs():
        for item in docs:
            rid = tuple(item['rid'])
            yield (item['text'], rid)

    block_dir = os.path.join(index_out_dir, 'blocks')
    total = build_spimi_blocks(iter_docs(), block_dir, block_max_docs=200, do_stem=True)
    print(f"Built blocks with {total} docs in {block_dir}")
    merge_blocks(block_dir, index_out_dir)
    print(f"Merged blocks into {index_out_dir}")


def query_command(index_dir: str, query: str, k: int = 10):
    res = search_topk(index_dir, query, k=k, do_stem=True)
    print(json.dumps(res, indent=2, ensure_ascii=False))


def main():
    if len(sys.argv) < 2:
        usage(); return
    cmd = sys.argv[1]
    if cmd == 'build' and len(sys.argv) >= 5:
        table_dir, column, index_out = sys.argv[2], sys.argv[3], sys.argv[4]
        build_command(table_dir, column, index_out)
    elif cmd == 'query' and len(sys.argv) >= 4:
        index_dir, q = sys.argv[2], sys.argv[3]
        k = int(sys.argv[4]) if len(sys.argv) >= 5 else 10
        query_command(index_dir, q, k)
    else:
        usage()


if __name__ == '__main__':
    main()
