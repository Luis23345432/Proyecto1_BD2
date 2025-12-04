from __future__ import annotations

import os
import shutil
import json

from engine import DatabaseEngine
from parser.runner import run_sql
from indexes.spimi import build_spimi_blocks, merge_blocks, search_topk


def test_spimi_build_with_multiple_columns(tmp_path):
    root = os.path.abspath('.')
    user = 'spimi_multi_user'
    dbname = 'spimi_multi_db'
    engine = DatabaseEngine(root)

    # Cleanup
    base_db_dir = os.path.join(root, 'data', 'users', user, 'databases', dbname)
    if os.path.exists(base_db_dir):
        shutil.rmtree(base_db_dir)

    # Create table with two textual columns
    create_sql = """
    CREATE TABLE Docs (
        id INT KEY,
        title VARCHAR[64] INDEX FULLTEXT,
        body VARCHAR INDEX FULLTEXT
    )
    """
    run_sql(root, user, dbname, create_sql)

    # Insert a few rows
    rows = [
        (1, 'Amor en la ciudad', 'Crónica de tiempos de guerra y paz'),
        (2, 'Gastronomía peruana', 'Pollo a la brasa y ají de gallina'),
        (3, 'Relatos de viaje', 'Amor, aventura y caminos lejanos'),
    ]
    for r in rows:
        t, b = r[1].replace("'", "''"), r[2].replace("'", "''")
        ins = f"INSERT INTO Docs (id, title, body) VALUES ({r[0]}, '{t}', '{b}')"
        run_sql(root, user, dbname, ins)

    # Build SPIMI index manually concatenating title+body to simulate API multi-columns
    db = engine.get_database(user, dbname)
    t = db.get_table('Docs')
    table_dir = t.base_dir
    block_dir = os.path.join(table_dir, 'spimi_blocks')
    index_dir = os.path.join(table_dir, 'spimi_index')
    if os.path.exists(block_dir):
        shutil.rmtree(block_dir)
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    os.makedirs(block_dir, exist_ok=True)
    os.makedirs(index_dir, exist_ok=True)

    def docs():
        pc = t.datafile.page_count()
        for pid in range(pc):
            page = t.datafile.read_page(pid)
            recs = page.iter_records()
            for slot, rec in enumerate(recs):
                rid = (pid, slot)
                text = f"{rec.get('title','')} {rec.get('body','')}".strip()
                if text:
                    yield (text, rid)

    total = build_spimi_blocks(docs(), block_dir, block_max_docs=1, do_stem=True)
    merge_blocks(block_dir, index_dir, total_docs=total)

    # Query should find rows mentioning 'amor' in title or body
    results = search_topk(index_dir, 'amor tiempos', k=5, do_stem=True)
    # Expect at least one match
    assert len(results) >= 1
