from __future__ import annotations

import json
import math
import os
import urllib.parse
from typing import Dict, Iterable, List, Tuple, Any

from .inverted_index import tokenize

# Simple SPIMI-based inverted index implementation
# - Builds blocks (term -> postings) from a stream of documents
# - Writes block files (JSON)
# - Merges block files into a final on-disk index organized per-term
# - Stores meta (N docs, doc norms) and per-term files under `index_dir`

DocID = str  # string like "page_slot" or numeric id as string


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
    """Create SPIMI blocks from an iterable of (text, rid) pairs.

    Each block is a JSON file mapping term -> list of [docid, tf]
    """
    _ensure_dir(block_dir)
    block = {}
    docs_in_block = 0
    block_id = 0
    total_docs = 0

    def write_block(bid: int, bdata: Dict[str, Dict[str, int]]):
        path = os.path.join(block_dir, f"block_{bid}.json")
        # bdata: term -> {docid: tf, ...}
        serial = {t: [[docid, tf] for docid, tf in postings.items()] for t, postings in bdata.items()}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serial, f, ensure_ascii=False)

    for text, rid in docs:
        total_docs += 1
        terms = tokenize(text, do_stem=do_stem)
        docid = _docid_to_str(rid)
        docs_in_block += 1

        # count tf per document
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
    """Merge JSON block files into per-term files in `index_dir/terms/` and create meta.json.

    Final layout:
      index_dir/
        meta.json  # {N: int}
        terms/
          <term>.json -> {"df": int, "postings": [[docid, tf], ...]}
    """
    _ensure_dir(index_dir)
    terms_dir = os.path.join(index_dir, "terms")
    _ensure_dir(terms_dir)

    # Gather block files
    block_files = [os.path.join(block_dir, f) for f in os.listdir(block_dir) if f.endswith('.json')]
    term_acc: Dict[str, Dict[str, int]] = {}

    # Read each block and accumulate per-term postings in temporary on-disk merges.
    # For moderate sizes we can merge in-memory per-term; for larger we should stream.
    for bf in block_files:
        with open(bf, 'r', encoding='utf-8') as f:
            data = json.load(f)
        for term, posting_list in data.items():
            acc = term_acc.setdefault(term, {})
            for docid, tf in posting_list:
                acc[docid] = acc.get(docid, 0) + int(tf)

    # Write final per-term files
    # compute N (total distinct docs) or use provided total_docs
    if total_docs is None:
        docs_seen = set()
        for term, postings in term_acc.items():
            for docid in postings.keys():
                docs_seen.add(docid)
        N = len(docs_seen)
    else:
        N = int(total_docs)

    for term, postings in term_acc.items():
        # sanitize term for filename
        safe_term = urllib.parse.quote_plus(term)
        pf = os.path.join(terms_dir, f"{safe_term}.json")
        # sort postings by docid for determinism
        posting_items = [[docid, tf] for docid, tf in sorted(postings.items())]
        payload = {"df": len(postings), "postings": posting_items}
        with open(pf, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False)

    # compute doc norms
    # norm(doc) = sqrt(sum_t (tfidf_{t,d})^2) ; tfidf uses 1+log(tf) * idf (idf = log(N/df))
    # Build a mapping docid -> sumsq
    doc_sumsq: Dict[DocID, float] = {}
    for term, postings in term_acc.items():
        df = len(postings)
        if df == 0:
            continue
        idf = math.log((N + 1) / df)  # smoothing
        for docid, tf in postings.items():
            tfw = 1.0 + math.log(float(tf)) if tf > 0 else 0.0
            w = tfw * idf
            doc_sumsq[docid] = doc_sumsq.get(docid, 0.0) + w * w

    doc_norms = {docid: math.sqrt(s) for docid, s in doc_sumsq.items()}

    meta = {"N": N, "num_terms": len(term_acc), "doc_norms": doc_norms}
    with open(os.path.join(index_dir, 'meta.json'), 'w', encoding='utf-8') as f:
        json.dump(meta, f, ensure_ascii=False)


def load_term_postings(index_dir: str, term: str) -> Tuple[int, List[Tuple[DocID, int]]]:
    terms_dir = os.path.join(index_dir, 'terms')
    # term filenames are URL-quoted
    safe_term = urllib.parse.quote_plus(term)
    pf = os.path.join(terms_dir, f"{safe_term}.json")
    if not os.path.exists(pf):
        return 0, []
    with open(pf, 'r', encoding='utf-8') as f:
        data = json.load(f)
    postings = [(docid, int(tf)) for docid, tf in data.get('postings', [])]
    return int(data.get('df', 0)), postings


def search_topk(index_dir: str, query: str, k: int = 10, do_stem: bool = False) -> List[Tuple[DocID, float]]:
    """Compute top-k by cosine similarity using on-disk term files.

    This function reads postings only for the query terms.
    """
    # load meta
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

    # query tf
    q_tf: Dict[str, int] = {}
    for t in q_terms:
        q_tf[t] = q_tf.get(t, 0) + 1

    # compute query weights
    q_weights: Dict[str, float] = {}
    for t, tf in q_tf.items():
        # load df
        df, postings = load_term_postings(index_dir, t)
        if df == 0:
            continue
        idf = math.log((N + 1) / df)
        tfw = 1.0 + math.log(float(tf)) if tf > 0 else 0.0
        q_weights[t] = tfw * idf

    if not q_weights:
        return []

    # accumulate dot-products
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

    # divide by document norms
    doc_norms = meta.get('doc_norms', {})
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
