"""Índice invertido SPIMI (Single-Pass In-Memory Indexing).

Implementa construcción de índice invertido para grandes colecciones:
- Construye bloques parciales del índice en memoria.
- Fusiona bloques en un índice final organizado por término.
- Almacena archivos por término para búsquedas eficientes.
- Calcula normas de documentos para ranking TF-IDF.
- Soporta búsqueda top-k con similitud coseno.
"""
from __future__ import annotations

import json
import math
import os
import urllib.parse
import heapq
from typing import Dict, Iterable, List, Tuple, Any, Set

from .inverted_index import tokenize

DocID = str


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _docid_to_str(docid: Tuple[int, int]) -> DocID:
    return f"{docid[0]}_{docid[1]}"


def build_spimi_blocks(
    docs: Iterable[Tuple[Any, Tuple[int, int]]],
    block_dir: str,
    block_max_docs: int = 500,
    do_stem: bool = False,
):
    """Construye bloques SPIMI desde un flujo de documentos.

    Cada bloque es un archivo JSON que mapea términos a listas de [docid, tf].
    
    Args:
        docs: Iterable de tuplas (texto, rid).
        block_dir: Directorio donde guardar los bloques.
        block_max_docs: Número máximo de documentos por bloque.
        do_stem: Si aplicar stemming a los tokens.
    
    Returns:
        Número total de documentos procesados.
    """
    _ensure_dir(block_dir)
    block = {}
    docs_in_block = 0
    block_id = 0
    total_docs = 0

    def write_block(bid: int, bdata: Dict[str, Dict[str, int]]):
        path = os.path.join(block_dir, f"block_{bid}.json")
        serial = {t: [[docid, tf] for docid, tf in postings.items()] for t, postings in bdata.items()}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serial, f, ensure_ascii=False)

    for text, rid in docs:
        total_docs += 1
        terms = tokenize(text, do_stem=do_stem)
        docid = _docid_to_str(rid)
        docs_in_block += 1

        counts: Dict[str, int] = {}
        for t in terms:
            counts[t] = counts.get(t, 0) + 1

        for t, tf in counts.items():
            posting = block.setdefault(t, {})
            posting[docid] = posting.get(docid, 0) + tf

        if docs_in_block >= block_max_docs:
            write_block(block_id, block)
            block_id += 1
            block = {}
            docs_in_block = 0

    if block:
        write_block(block_id, block)

    return total_docs


def merge_blocks(block_dir: str, index_dir: str, total_docs: int | None = None) -> None:
    """Fusiona bloques JSON en archivos por término y crea meta.json.

    Estructura final:
        index_dir/
            meta.json  # {N: int, doc_norms: {...}}
            terms/
                <term>.json -> {"df": int, "postings": [[docid, tf], ...]}
    
    Args:
        block_dir: Directorio con archivos de bloques.
        index_dir: Directorio de salida para el índice final.
        total_docs: Número total de documentos (opcional).
    """
    _ensure_dir(index_dir)
    terms_dir = os.path.join(index_dir, "terms")
    _ensure_dir(terms_dir)

    block_files = [os.path.join(block_dir, f) for f in os.listdir(block_dir) if f.endswith('.json')]
    if not block_files:
        return
    print(f"Merging {len(block_files)} block(s) from {block_dir} into {index_dir}")

    block_iters: List[Dict[str, Any]] = []
    for bf in block_files:
        with open(bf, 'r', encoding='utf-8') as f:
            data = json.load(f)
        items = list(data.items())
        items.sort(key=lambda x: x[0])
        block_iters.append({'items': items, 'idx': 0, 'file': bf})

    heap: List[Tuple[str, int]] = []
    for i, b in enumerate(block_iters):
        if b['items']:
            term0 = b['items'][0][0]
            heapq.heappush(heap, (term0, i))

    num_terms = 0
    while heap:
        term, bidx = heapq.heappop(heap)
        agg: Dict[str, int] = {}

        b = block_iters[bidx]
        idx = b['idx']
        t, postings = b['items'][idx]
        assert t == term
        for docid, tf in postings:
            agg[docid] = agg.get(docid, 0) + int(tf)
        b['idx'] += 1
        if b['idx'] < len(b['items']):
            next_term = b['items'][b['idx']][0]
            heapq.heappush(heap, (next_term, bidx))

        while heap and heap[0][0] == term:
            _, other_bidx = heapq.heappop(heap)
            ob = block_iters[other_bidx]
            oidx = ob['idx']
            ot, opostings = ob['items'][oidx]
            assert ot == term
            for docid, tf in opostings:
                agg[docid] = agg.get(docid, 0) + int(tf)
            ob['idx'] += 1
            if ob['idx'] < len(ob['items']):
                heapq.heappush(heap, (ob['items'][ob['idx']][0], other_bidx))

        safe_term = urllib.parse.quote_plus(term)
        pf = os.path.join(terms_dir, f"{safe_term}.json")
        posting_items = [[docid, tf] for docid, tf in sorted(agg.items())]
        payload = {"df": len(agg), "postings": posting_items}
        with open(pf, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)
        num_terms += 1

    print(f"Merged {num_terms} terms. Computing doc norms and writing meta.json")
    if total_docs is None:
        docs_seen = set()
        for fname in os.listdir(terms_dir):
            pf = os.path.join(terms_dir, fname)
            with open(pf, 'r', encoding='utf-8') as f:
                data = json.load(f)
            for docid, tf in data.get('postings', []):
                docs_seen.add(docid)
        N = len(docs_seen)
    else:
        N = int(total_docs)

    doc_sumsq: Dict[DocID, float] = {}
    num_terms = 0
    for fname in os.listdir(terms_dir):
        pf = os.path.join(terms_dir, fname)
        with open(pf, 'r', encoding='utf-8') as f:
            data = json.load(f)
        df = int(data.get('df', 0))
        if df == 0:
            continue
        num_terms += 1
        idf = math.log((N + 1) / df)
        for docid, tf in data.get('postings', []):
            tfv = int(tf)
            tfw = 1.0 + math.log(float(tfv)) if tfv > 0 else 0.0
            w = tfw * idf
            doc_sumsq[docid] = doc_sumsq.get(docid, 0.0) + w * w

    doc_norms = {docid: math.sqrt(s) for docid, s in doc_sumsq.items()}

    SHARD_THRESHOLD = 50000
    if len(doc_norms) > SHARD_THRESHOLD:
        shard_dir = os.path.join(index_dir, 'doc_norms')
        _ensure_dir(shard_dir)
        shard_count = 256

        buckets: List[Dict[str, float]] = [dict() for _ in range(shard_count)]
        import hashlib

        def shard_index(docid: str) -> int:
            h = hashlib.sha1(docid.encode('utf-8')).digest()[0]
            return int(h)

        for docid, norm in doc_norms.items():
            idx = shard_index(docid)
            buckets[idx][docid] = norm

        for i, bucket in enumerate(buckets):
            if not bucket:
                continue
            shard_name = f"{i:02x}.json"
            with open(os.path.join(shard_dir, shard_name), 'w', encoding='utf-8') as f:
                json.dump(bucket, f, ensure_ascii=False)

        meta = {"N": N, "num_terms": num_terms, "doc_norms_sharded": True, "shards": 256}
        with open(os.path.join(index_dir, 'meta.json'), 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False)
    else:
        meta = {"N": N, "num_terms": num_terms, "doc_norms": doc_norms}
        with open(os.path.join(index_dir, 'meta.json'), 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False)


def load_term_postings(index_dir: str, term: str) -> Tuple[int, List[Tuple[DocID, int]]]:
    """Carga las postings de un término desde el índice en disco.
    
    Returns:
        Tupla (df, postings) donde df es la frecuencia de documento y
        postings es una lista de (docid, tf).
    """
    terms_dir = os.path.join(index_dir, 'terms')
    safe_term = urllib.parse.quote_plus(term)
    pf = os.path.join(terms_dir, f"{safe_term}.json")
    if not os.path.exists(pf):
        return 0, []
    with open(pf, 'r', encoding='utf-8') as f:
        data = json.load(f)
    postings = [(docid, int(tf)) for docid, tf in data.get('postings', [])]
    return int(data.get('df', 0)), postings


def search_topk(index_dir: str, query: str, k: int = 10, do_stem: bool = False) -> List[Tuple[DocID, float]]:
    """Calcula los top-k documentos más relevantes usando similitud coseno.

    Lee solo las postings de los términos de la consulta desde disco.
    
    Args:
        index_dir: Directorio del índice SPIMI.
        query: Cadena de consulta.
        k: Número de resultados a retornar.
        do_stem: Si aplicar stemming.
    
    Returns:
        Lista de tuplas (docid, score) ordenada por score descendente.
    """
    meta_path = os.path.join(index_dir, 'meta.json')
    if not os.path.exists(meta_path):
        return []
    with open(meta_path, 'r', encoding='utf-8') as f:
        meta = json.load(f)
    N = int(meta.get('N', 0))
    if N == 0:
        return []

    q_terms = tokenize(query, do_stem=do_stem)
    if not q_terms:
        return []

    q_tf: Dict[str, int] = {}
    for t in q_terms:
        q_tf[t] = q_tf.get(t, 0) + 1

    q_weights: Dict[str, float] = {}
    for t, tf in q_tf.items():
        df, postings = load_term_postings(index_dir, t)
        if df == 0:
            continue
        idf = math.log((N + 1) / df)
        tfw = 1.0 + math.log(float(tf)) if tf > 0 else 0.0
        q_weights[t] = tfw * idf

    if not q_weights:
        return []

    scores: Dict[DocID, float] = {}
    for t, qw in q_weights.items():
        df, postings = load_term_postings(index_dir, t)
        if df == 0:
            continue
        idf = math.log((N + 1) / df)
        for docid, tf in postings:
            tfw = 1.0 + math.log(float(tf)) if tf > 0 else 0.0
            w = tfw * idf
            scores[docid] = scores.get(docid, 0.0) + qw * w

    doc_norms = meta.get('doc_norms', {})
    if meta.get('doc_norms_sharded'):
        import hashlib
        shard_dir = os.path.join(index_dir, 'doc_norms')
        needed_shards: Set[int] = set()
        for docid in scores.keys():
            h = hashlib.sha1(docid.encode('utf-8')).digest()[0]
            needed_shards.add(int(h))
        loaded: Dict[str, float] = {}
        for i in needed_shards:
            shard_path = os.path.join(shard_dir, f"{i:02x}.json")
            if not os.path.exists(shard_path):
                continue
            with open(shard_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            loaded.update({str(k): float(v) for k, v in data.items()})
        doc_norms = loaded
    q_norm = math.sqrt(sum(v * v for v in q_weights.values()))
    ranked: List[Tuple[DocID, float]] = []
    for docid, dot in scores.items():
        dn = float(doc_norms.get(docid, 0.0))
        if dn == 0 or q_norm == 0:
            continue
        sim = dot / (dn * q_norm)
        ranked.append((docid, sim))

    ranked.sort(key=lambda x: x[1], reverse=True)
    return ranked[:k]
