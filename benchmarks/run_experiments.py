from __future__ import annotations

import os
import time
import json
from typing import List, Tuple
from pathlib import Path
import sys

# Ensure repo root is on sys.path so local package imports work
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import numpy as np

from engine import DatabaseEngine
from parser.runner import run_sql
from indexes.spimi import build_spimi_blocks, merge_blocks, search_topk
from multimedia.codebook import train_codebook
from multimedia.features_image import batch_extract_sift
from multimedia.bow import quantize_descriptors, compute_df, save_bow_artifacts
from multimedia.knn_sequential import search_sequential
from multimedia.inv_index import build_inverted_index, search_inverted


def ensure_db_table_text(root: str, user: str, dbname: str, N: int) -> Tuple[str, str]:
    """Create a text table and insert N synthetic rows.
    Returns (table_name, index_dir)
    """
    table = "DocsExp"
    # Create DB + table
    run_sql(root, user, dbname, f"CREATE TABLE {table} (id INT KEY, title VARCHAR[64] INDEX FULLTEXT, body VARCHAR INDEX FULLTEXT)")
    # Insert synthetic rows
    for i in range(N):
        t = f"Producto {i} oferta calidad"
        b = f"Este es un texto de prueba con amor y aventura {i}"
        run_sql(root, user, dbname, f"INSERT INTO {table} (id, title, body) VALUES ({i}, '{t}', '{b}')")
    # Build SPIMI over concatenated columns (manual path)
    engine = DatabaseEngine(root)
    db = engine.get_database(user, dbname)
    tab = db.get_table(table)
    table_dir = tab.base_dir
    block_dir = os.path.join(table_dir, 'spimi_blocks')
    index_dir = os.path.join(table_dir, 'spimi_index')
    os.makedirs(block_dir, exist_ok=True)
    os.makedirs(index_dir, exist_ok=True)

    def docs():
        pc = tab.datafile.page_count()
        for pid in range(pc):
            page = tab.datafile.read_page(pid)
            for slot, rec in enumerate(page.iter_records()):
                rid = (pid, slot)
                text = f"{rec.get('title','')} {rec.get('body','')}".strip()
                if text:
                    yield (text, rid)

    total = build_spimi_blocks(docs(), block_dir, block_max_docs=1000, do_stem=True)
    merge_blocks(block_dir, index_dir, total_docs=total)
    return table, index_dir


def measure_text(root: str, user: str, dbname: str, Ns: List[int], query: str) -> List[Tuple[int, float, int]]:
    """Return [(N, ms, topk_count)] for our SPIMI implementation.
    """
    results = []
    for N in Ns:
        table, index_dir = ensure_db_table_text(root, user, f"db_text_{N}", N)
        t0 = time.perf_counter()
        topk = search_topk(index_dir, query, k=8, do_stem=True)
        dt = (time.perf_counter() - t0) * 1000.0
        results.append((N, dt, len(topk)))
    return results


def measure_multimedia_image(root: str, data_root: str, Ns: List[int]) -> List[Tuple[int, float, float]]:
    """Return [(N, ms_seq, ms_inv)] measuring sequential vs inverted KNN.
    Uses the first image as query.
    """
    # Collect image paths
    files = []
    for dirpath, _, filenames in os.walk(data_root):
        for f in filenames:
            if f.lower().endswith(('.jpg', '.jpeg', '.png')):
                files.append(os.path.join(dirpath, f))
    files.sort()
    if not files:
        raise RuntimeError("No image files found in data_root")

    results = []
    for N in Ns:
        subset = files[:N]
        ids, descs = batch_extract_sift(subset)
        if not ids:
            results.append((N, 0.0, 0.0))
            continue
        # Train codebook (k fixed for experiment)
        samples = np.vstack(descs)
        km = train_codebook(samples, k=512, batch_size=256, seed=42)
        centroids = km.cluster_centers_.astype(np.float32)
        # Quantize
        hists = [quantize_descriptors(d, centroids, top_m=3, sigma=1.0) for d in descs]
        df = compute_df(hists)

        # Save BoW artifacts in tmp dir
        bow_dir = os.path.join(root, 'data', 'multimedia', 'image', 'bow_exp')
        os.makedirs(bow_dir, exist_ok=True)
        # Clean previous
        for i in range(len(ids)):
            p = os.path.join(bow_dir, f"bow_{i}.npz")
            if os.path.exists(p):
                os.remove(p)
        save_bow_artifacts(bow_dir, hists, ids, df)

        # Build inverted index
        inv_dir = os.path.join(root, 'data', 'multimedia', 'image', 'inv_exp')
        os.makedirs(inv_dir, exist_ok=True)
        # Clean previous postings
        for f in os.listdir(inv_dir):
            if f.startswith('cw_'):
                os.remove(os.path.join(inv_dir, f))
        build_inverted_index(ids, hists, inv_dir)

        # Query: first doc
        q = hists[0]
        t0 = time.perf_counter()
        _ = search_sequential(q, bow_dir, top_k=8)
        ms_seq = (time.perf_counter() - t0) * 1000.0
        t0 = time.perf_counter()
        _ = search_inverted(q, inv_dir, top_k=8)
        ms_inv = (time.perf_counter() - t0) * 1000.0
        results.append((N, ms_seq, ms_inv))
    return results


def main():
    root = os.path.abspath('.')
    user = 'exp_user'
    # Text experiment
    Ns = [1000, 2000, 4000]
    text_query = 'amor aventura'
    text_rows = measure_text(root, user, 'exp_db_text', Ns, text_query)

    # Multimedia experiment (image)
    data_root = os.environ.get('EXP_IMAGE_ROOT', os.path.join('datasets', 'fashion-product-images-dataset', 'fashion-product-images'))
    mm_rows = []
    try:
        mm_rows = measure_multimedia_image(root, data_root, Ns)
    except Exception as e:
        mm_rows = [(n, 0.0, 0.0) for n in Ns]
        print('Multimedia experiment skipped:', e)

    report = {
        'text': [{'N': N, 'ms': round(ms, 2), 'count': c} for (N, ms, c) in text_rows],
        'multimedia_image': [{'N': N, 'ms_seq': round(ms_seq, 2), 'ms_inv': round(ms_inv, 2)} for (N, ms_seq, ms_inv) in mm_rows],
        'notes': 'Use pgVector/FAISS externally for PostgreSQL/ANN comparisons.'
    }
    out = os.path.join(root, 'benchmarks', 'report.json')
    with open(out, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    print('Saved', out)


if __name__ == '__main__':
    main()
