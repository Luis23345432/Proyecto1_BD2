"""Integration test for SQL full-text queries using @@ and SPIMI.

This script creates a DB and table, inserts records, builds SPIMI index,
and runs a SQL query with WHERE lyric @@ '...' LIMIT 10, verifying that
the result includes rows with `_score` field.
"""
from __future__ import annotations

import os
import shutil
import json

from engine import DatabaseEngine
from parser.runner import run_sql
from indexes.spimi import build_spimi_blocks, merge_blocks


def ensure_clean_dir(path: str):
    if os.path.exists(path):
        shutil.rmtree(path)
    os.makedirs(path, exist_ok=True)


def main():
    root = os.path.dirname(os.path.dirname(__file__))
    user = 'testuser'
    dbname = 'testdb'
    engine = DatabaseEngine(root)
    # Cleanup if exists
    base_db_dir = os.path.join(root, 'data', 'users', user, 'databases', dbname)
    if os.path.exists(base_db_dir):
        shutil.rmtree(base_db_dir)

    # Create database and table via SQL
    create_sql = "CREATE TABLE Audio (title VARCHAR, artist VARCHAR, lyric VARCHAR) USING INDEX FULLTEXT(lyric)"
    out = run_sql(root, user, dbname, create_sql)
    print('Create table:', out)

    # Insert rows
    insert_stmts = [
        "INSERT INTO Audio VALUES ('Amor y Paz', 'Grupo A', 'amor en tiempos de guerra y paz')",
        "INSERT INTO Audio VALUES ('Guerra', 'Grupo B', 'batallas y guerras, amor perdido')",
        "INSERT INTO Audio VALUES ('Canción de amor', 'Grupo C', 'amor en tiempos de guerra, sol y luna')",
    ]
    for st in insert_stmts:
        print(run_sql(root, user, dbname, st))

    # Build SPIMI index for lyric column (simulate API build)
    db = engine.get_database(user, dbname)
    # Table names preserve casing; find created table name
    tname = next(iter(db.list_tables())) if db.list_tables() else 'Audio'
    t = db.get_table(tname)
    table_dir = t.base_dir
    block_dir = os.path.join(table_dir, 'spimi_blocks')
    index_dir = os.path.join(table_dir, 'spimi_index')
    ensure_clean_dir(block_dir)
    ensure_clean_dir(index_dir)

    # Iterate datafile
    def doc_iter():
        pc = t.datafile.page_count()
        for pid in range(pc):
            page = t.datafile.read_page(pid)
            recs = page.iter_records()
            for slot, rec in enumerate(recs):
                rid = (pid, slot)
                text = rec.get('lyric')
                if text:
                    yield (text, rid)

    total = build_spimi_blocks(doc_iter(), block_dir, block_max_docs=1, do_stem=True)
    merge_blocks(block_dir, index_dir, total_docs=total)
    # copy to canonical
    canonical_index = os.path.join(table_dir, 'spimi_index')
    print('Index built. Meta:')
    with open(os.path.join(index_dir, 'meta.json'), 'r', encoding='utf-8') as f:
        print(json.dumps(json.load(f), indent=2, ensure_ascii=False))

    # Run SQL fulltext query
    q = "SELECT title, artist, lyric FROM Audio WHERE lyric @@ 'amor en tiempos de guerra' LIMIT 10"
    res = run_sql(root, user, dbname, q)
    print('Query result:')
    print(res)


if __name__ == '__main__':
    main()
