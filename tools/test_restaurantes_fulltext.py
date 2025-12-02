from __future__ import annotations

import os
import csv
import json
import shutil

from engine import DatabaseEngine
from parser.runner import run_sql
from indexes.spimi import build_spimi_blocks, merge_blocks, search_topk


def main():
    root = os.path.abspath('.')
    user='spimi_test_user'
    db='spimi_test_db'
    table='Restaurantes'

    engine = DatabaseEngine(root)

    # cleanup DB
    db_dir = os.path.join(root, 'data','users',user,'databases', db)
    if os.path.exists(db_dir):
        shutil.rmtree(db_dir)

    # Create table via SQL
    create_sql = f"CREATE TABLE {table} (id INT, name VARCHAR, description VARCHAR) USING INDEX FULLTEXT(description)"
    print(run_sql(root, user, db, create_sql))

    # Load CSV
    csv_path = os.path.join('postman','restaurantes.csv')
    rows = []
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append({
                'id': int(r['id']),
                'name': r['name'],
                'description': r['description']
            })

    for r in rows:
        # escape single quotes by doubling them for SQL literal
        name_esc = r['name'].replace("'", "''")
        desc_esc = r['description'].replace("'", "''")
        ins = f"INSERT INTO {table} VALUES ({r['id']}, '{name_esc}', '{desc_esc}')"
        run_sql(root, user, db, ins)

    # Build SPIMI index for description
    db_obj = engine.get_database(user, db)
    t = db_obj.get_table(table)
    block_dir = os.path.join(t.base_dir, 'spimi_blocks')
    index_dir = os.path.join(t.base_dir, 'spimi_index')
    if os.path.exists(block_dir):
        shutil.rmtree(block_dir)
    if os.path.exists(index_dir):
        shutil.rmtree(index_dir)
    os.makedirs(block_dir, exist_ok=True)
    os.makedirs(index_dir, exist_ok=True)

    def doc_iter():
        pc = t.datafile.page_count()
        for pid in range(pc):
            page = t.datafile.read_page(pid)
            for slot, rec in enumerate(page.iter_records()):
                rid=(pid, slot)
                text = rec.get('description')
                if text:
                    yield (text, rid)

    total = build_spimi_blocks(doc_iter(), block_dir, block_max_docs=200, do_stem=True)
    merge_blocks(block_dir, index_dir, total_docs=total)

    # Search
    results = search_topk(index_dir, 'pollo', k=10, do_stem=True)
    print('SPIMI results:', results)

    # Run SQL query
    q = f"SELECT id, name, description FROM {table} WHERE description @@ 'pollo' LIMIT 10"
    res = run_sql(root, user, db, q)
    print('SQL query results:', json.dumps(res, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
