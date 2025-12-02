"""Quick test for SPIMI merge with priority queue.

Creates small blocks from example docs, merges them and prints meta + term files.
"""
from __future__ import annotations

import os
import shutil
import json

from indexes.spimi import build_spimi_blocks, merge_blocks
from indexes.inverted_index import tokenize


def test_merge(tmpdir: str):
    docs = [
        ("the quick brown fox", (0, 0)),
        ("jumps over the lazy dog", (0, 1)),
        ("the quick blue hare", (0, 2)),
    ]
    # prepare directories
    block_dir = os.path.join(tmpdir, 'blocks')
    index_dir = os.path.join(tmpdir, 'index')
    if os.path.exists(tmpdir):
        shutil.rmtree(tmpdir)
    os.makedirs(block_dir, exist_ok=True)
    os.makedirs(index_dir, exist_ok=True)

    total = build_spimi_blocks(docs, block_dir, block_max_docs=1, do_stem=False)
    print(f"Total docs: {total}")
    merge_blocks(block_dir, index_dir, total_docs=total)
    print('Meta:')
    with open(os.path.join(index_dir, 'meta.json'), 'r', encoding='utf-8') as f:
        print(json.dumps(json.load(f), indent=2, ensure_ascii=False))
    print('Terms:')
    terms_dir = os.path.join(index_dir, 'terms')
    for fname in os.listdir(terms_dir):
        pf = os.path.join(terms_dir, fname)
        with open(pf, 'r', encoding='utf-8') as f:
            data = json.load(f)
        print(fname, json.dumps(data, ensure_ascii=False))
    # Basic checks
    expected_terms = set()
    for t, _ in docs:
        toks = tokenize(t, do_stem=False)
        for tok in toks:
            expected_terms.add(tok)
    files = os.listdir(terms_dir)
    assert len(files) == len(expected_terms), f"expected {len(expected_terms)} terms in index, got {len(files)}"
    print("Basic checks OK")


if __name__ == '__main__':
    import tempfile
    tempdir = tempfile.mkdtemp(prefix='spimi_test_')
    test_merge(tempdir)
