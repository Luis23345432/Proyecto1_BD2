import logging
from typing import Dict, List, Tuple

import numpy as np
import pickle
from sklearn.metrics import pairwise_distances


logger = logging.getLogger(__name__)


def quantize_descriptors(descriptors: np.ndarray, centroids: np.ndarray, top_m: int = 3, sigma: float = 1.0) -> np.ndarray:
    if descriptors.shape[0] == 0:
        return np.zeros((centroids.shape[0],), dtype=np.float32)
    # Soft-assignment: contribute to top_m nearest centroids with Gaussian weights
    dists = pairwise_distances(descriptors, centroids, metric="euclidean")  # shape (n_desc, k)
    k = centroids.shape[0]
    hist = np.zeros((k,), dtype=np.float32)
    m = max(1, min(top_m, k))
    # For each descriptor, find m smallest distances
    idx = np.argpartition(dists, m - 1, axis=1)[:, :m]
    # Gather distances for those indices
    rows = np.arange(dists.shape[0])[:, None]
    selected = dists[rows, idx]
    # Convert to weights: w = exp(-d^2 / (2*sigma^2)); normalize per descriptor
    w = np.exp(- (selected ** 2) / (2.0 * (sigma ** 2) + 1e-12)).astype(np.float32)
    norm = np.sum(w, axis=1, keepdims=True)
    norm[norm == 0.0] = 1.0
    w = w / norm
    # Accumulate into histogram
    for i in range(idx.shape[0]):
        for j in range(m):
            hist[idx[i, j]] += w[i, j]
    return hist


def compute_df(histograms: List[np.ndarray]) -> np.ndarray:
    # Document frequency per codeword
    df = np.zeros((histograms[0].shape[0],), dtype=np.int32)
    for h in histograms:
        df += (h > 0).astype(np.int32)
    return df


def compute_tfidf(h: np.ndarray, df: np.ndarray, n_docs: int) -> np.ndarray:
    # tf-idf with smoothing similar to text pipeline
    idf = np.log((n_docs + 1) / (df + 1)) + 1.0
    # Use sublinear tf for stability
    tf = np.log1p(h)
    w = tf * idf
    # L2 normalize
    norm = np.linalg.norm(w)
    if norm > 0:
        w = w / norm
    return w.astype(np.float32)


def save_bow_artifacts(out_dir: str, histograms: List[np.ndarray], doc_ids: List[str], df: np.ndarray):
    import os
    os.makedirs(out_dir, exist_ok=True)
    # Save histograms per doc to keep memory bounded
    for i, (doc_id, h) in enumerate(zip(doc_ids, histograms)):
        np.savez_compressed(os.path.join(out_dir, f"bow_{i}.npz"), doc_id=doc_id, hist=h)
    with open(os.path.join(out_dir, "doc_ids.pkl"), "wb") as f:
        pickle.dump(doc_ids, f)
    with open(os.path.join(out_dir, "df.pkl"), "wb") as f:
        pickle.dump(df, f)


def load_bow_artifacts(out_dir: str) -> Tuple[List[str], List[np.ndarray], np.ndarray]:
    import os
    with open(os.path.join(out_dir, "doc_ids.pkl"), "rb") as f:
        doc_ids = pickle.load(f)
    histograms = []
    for i in range(len(doc_ids)):
        obj = np.load(os.path.join(out_dir, f"bow_{i}.npz"))
        histograms.append(obj["hist"])  # type: ignore
    with open(os.path.join(out_dir, "df.pkl"), "rb") as f:
        df = pickle.load(f)
    return doc_ids, histograms, df
