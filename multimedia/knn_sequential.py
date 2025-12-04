import os
import pickle
from typing import List, Tuple

import numpy as np
from .bow import compute_df


def load_bow(out_dir: str):
    with open(os.path.join(out_dir, "doc_ids.pkl"), "rb") as f:
        doc_ids = pickle.load(f)
    hists = []
    for i in range(len(doc_ids)):
        obj = np.load(os.path.join(out_dir, f"bow_{i}.npz"))
        hists.append(obj["hist"])  # type: ignore
    # df.pkl may be stale if codebook changed; prefer to recompute from histograms
    return doc_ids, hists


def tfidf_normalize(h: np.ndarray, df: np.ndarray, n_docs: int) -> np.ndarray:
    # Sublinear tf and TF-IDF; guard against shape mismatch
    if df.shape[0] != h.shape[0]:
        # fallback: match lengths by truncating or padding zeros
        k = h.shape[0]
        df = df[:k] if df.shape[0] > k else np.pad(df, (0, k - df.shape[0]), constant_values=0)
    idf = np.log((n_docs + 1) / (df + 1)) + 1.0
    tf = np.log1p(h)
    w = tf * idf
    norm = np.linalg.norm(w)
    if norm > 0:
        w = w / norm
    return w.astype(np.float32)


def search_sequential(query_hist: np.ndarray, bow_dir: str, top_k: int = 10) -> List[Tuple[str, float]]:
    doc_ids, hists = load_bow(bow_dir)
    n_docs = len(doc_ids)
    # Ensure all histograms share the same length
    k = query_hist.shape[0]
    hists = [h if h.shape[0] == k else (h[:k] if h.shape[0] > k else np.pad(h, (0, k - h.shape[0]), constant_values=0.0)) for h in hists]
    # Compute fresh DF to avoid mismatches
    df = compute_df(hists)
    wq = tfidf_normalize(query_hist, df, n_docs)
    import heapq
    heap = []
    for i, h in enumerate(hists):
        wd = tfidf_normalize(h, df, n_docs)
        s = float(np.dot(wq, wd))
        if len(heap) < top_k:
            heapq.heappush(heap, (s, i))
        else:
            if s > heap[0][0]:
                heapq.heapreplace(heap, (s, i))
    result = sorted(heap, key=lambda x: -x[0])
    return [(doc_ids[i], float(s)) for s, i in result]
