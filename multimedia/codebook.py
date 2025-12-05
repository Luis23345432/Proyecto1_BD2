"""Módulo para el entrenamiento de codebooks visuales (diccionarios visuales).

Implementa el entrenamiento de diccionarios visuales mediante K-Means en mini-batch
para cuantizar descriptores locales (SIFT, MFCC) en un vocabulario de palabras visuales.
"""

import logging
from typing import Dict, List, Tuple

import numpy as np
from sklearn.cluster import MiniBatchKMeans
import pickle


logger = logging.getLogger(__name__)


def sample_descriptors(descriptor_lists: List[np.ndarray], per_object_cap: int = 2000, global_cap: int = 200000) -> np.ndarray:
    """Muestrea descriptores de múltiples objetos para el entrenamiento del codebook.
    
    Args:
        descriptor_lists: Lista de matrices de descriptores por objeto
        per_object_cap: Máximo de descriptores a tomar por objeto
        global_cap: Máximo total de descriptores a recolectar
        
    Returns:
        Matriz consolidada de descriptores muestreados
    """
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
    """Entrena un codebook visual usando K-Means en mini-batch.
    
    Args:
        samples: Matriz de descriptores muestreados (n_samples, dim)
        k: Número de clusters (tamaño del vocabulario)
        batch_size: Tamaño del mini-batch para K-Means
        seed: Semilla aleatoria para reproducibilidad
        
    Returns:
        Modelo K-Means entrenado
    """
    if samples.shape[0] == 0:
        raise ValueError("No samples provided for codebook training")
    km = MiniBatchKMeans(n_clusters=k, batch_size=batch_size, random_state=seed, n_init=5)
    km.fit(samples)
    return km


def save_codebook(km: MiniBatchKMeans, path: str, modality: str, dim: int):
    """Guarda el codebook entrenado con metadatos.
    
    Args:
        km: Modelo K-Means entrenado
        path: Ruta del archivo de salida
        modality: Tipo de modalidad ('image' o 'audio')
        dim: Dimensionalidad de los descriptores
    """
    meta = {
        "modality": modality,
        "k": km.n_clusters,
        "dim": dim,
        "inertia": float(km.inertia_),
    }
    with open(path, "wb") as f:
        pickle.dump({"model": km, "meta": meta}, f)


def load_codebook(path: str) -> Tuple[MiniBatchKMeans, Dict]:
    """Carga un codebook previamente entrenado.
    
    Args:
        path: Ruta del archivo del codebook
        
    Returns:
        Tupla (modelo, metadatos)
    """
    with open(path, "rb") as f:
        obj = pickle.load(f)
    return obj["model"], obj["meta"]
