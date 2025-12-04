import logging
import os
import pickle
from typing import Dict, List, Tuple

import numpy as np


logger = logging.getLogger(__name__)


def build_inverted_index(doc_ids: List[str], histograms: List[np.ndarray], out_dir: str):
    os.makedirs(out_dir, exist_ok=True)
    n_docs = len(doc_ids)
    k = histograms[0].shape[0]
    postings: Dict[int, List[Tuple[int, float]]] = {i: [] for i in range(k)}
    # DF
    df = np.zeros((k,), dtype=np.int32)
    for d_i, h in enumerate(histograms):
        active = np.where(h > 0)[0]
        df[active] += 1
    idf = np.log((n_docs + 1) / (df + 1)) + 1.0
    # TF-IDF normalized per doc
    for d_i, h in enumerate(histograms):
        w = h * idf
        norm = np.linalg.norm(w)
        if norm > 0:
            w = w / norm
        active = np.where(w > 0)[0]
        for cw in active:
            postings[cw].append((d_i, float(w[cw])))
    # Save blocks (simple single file per codeword for now)
    with open(os.path.join(out_dir, "doc_ids.pkl"), "wb") as f:
        pickle.dump(doc_ids, f)
    with open(os.path.join(out_dir, "idf.pkl"), "wb") as f:
        pickle.dump(idf, f)
    # term_to_block mapping trivial: each cw -> file cw.pkl
    term_to_block = {}
    for cw, plist in postings.items():
        path = os.path.join(out_dir, f"cw_{cw}.pkl")
        with open(path, "wb") as f:
            pickle.dump(plist, f)
        term_to_block[cw] = path
    with open(os.path.join(out_dir, "term_to_block.pkl"), "wb") as f:
        pickle.dump(term_to_block, f)


def search_inverted(query_hist: np.ndarray, index_dir: str, top_k: int = 10) -> List[Tuple[str, float]]:
    with open(os.path.join(index_dir, "doc_ids.pkl"), "rb") as f:
        doc_ids = pickle.load(f)
    with open(os.path.join(index_dir, "idf.pkl"), "rb") as f:
        idf = pickle.load(f)
    with open(os.path.join(index_dir, "term_to_block.pkl"), "rb") as f:
        t2b = pickle.load(f)
    # Validate dimensionality consistency
    if idf.shape[0] != query_hist.shape[0]:
        raise ValueError(
            f"Codebook dimensionality mismatch: query_hist has {query_hist.shape[0]} bins "
            f"but index was built with {idf.shape[0]} bins. Rebuild BoW and inverted index "
            f"using the same codebook size."
        )
    # Weight and normalize query
    wq = query_hist * idf
    nq = np.linalg.norm(wq)
    if nq > 0:
        wq = wq / nq
    scores = {}
    active = np.where(wq > 0)[0]
    for cw in active:
        path = t2b.get(int(cw))
        if not path:
            continue
        with open(path, "rb") as f:
            plist = pickle.load(f)  # List[Tuple[int, float]]
        wq_cw = float(wq[cw])
        for d_i, w_doc in plist:
            scores[d_i] = scores.get(d_i, 0.0) + wq_cw * w_doc
    # Top-K
    import heapq
    heap = []
    for d_i, s in scores.items():
        if len(heap) < top_k:
            heapq.heappush(heap, (s, d_i))
        else:
            if s > heap[0][0]:
                heapq.heapreplace(heap, (s, d_i))
    result = sorted(heap, key=lambda x: -x[0])
    return [(doc_ids[d_i], float(s)) for s, d_i in result]
