import logging
from typing import Dict, List, Tuple

import numpy as np
from sklearn.cluster import MiniBatchKMeans
import pickle


logger = logging.getLogger(__name__)


def sample_descriptors(descriptor_lists: List[np.ndarray], per_object_cap: int = 2000, global_cap: int = 200000) -> np.ndarray:
    samples = []
    total = 0
    for d in descriptor_lists:
        if d.shape[0] == 0:
            continue
        take = min(d.shape[0], per_object_cap)
        idx = np.random.default_rng(42).choice(d.shape[0], size=take, replace=False)
        samples.append(d[idx])
        total += take
        if total >= global_cap:
            break
    if not samples:
        return np.empty((0, 0), dtype=np.float32)
    return np.vstack(samples).astype(np.float32)


def train_codebook(samples: np.ndarray, k: int = 512, batch_size: int = 1000, seed: int = 42) -> MiniBatchKMeans:
    if samples.shape[0] == 0:
        raise ValueError("No samples provided for codebook training")
    km = MiniBatchKMeans(n_clusters=k, batch_size=batch_size, random_state=seed, n_init=5)
    km.fit(samples)
    return km


def save_codebook(km: MiniBatchKMeans, path: str, modality: str, dim: int):
    meta = {
        "modality": modality,
        "k": km.n_clusters,
        "dim": dim,
        "inertia": float(km.inertia_),
    }
    with open(path, "wb") as f:
        pickle.dump({"model": km, "meta": meta}, f)


def load_codebook(path: str) -> Tuple[MiniBatchKMeans, Dict]:
    with open(path, "rb") as f:
        obj = pickle.load(f)
    return obj["model"], obj["meta"]
