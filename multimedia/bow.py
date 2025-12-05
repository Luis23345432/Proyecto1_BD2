"""Módulo para la construcción de representaciones Bag-of-Words (BoW) visuales.

Este módulo implementa la cuantización de descriptores visuales (SIFT, MFCC, etc.)
en un diccionario visual (codebook) y la construcción de histogramas de palabras
visuales. Incluye ponderación TF-IDF para mejorar la discriminación entre documentos.
"""

import logging
from typing import Dict, List, Tuple

import numpy as np
import pickle
from sklearn.metrics import pairwise_distances


logger = logging.getLogger(__name__)


def quantize_descriptors(descriptors: np.ndarray, centroids: np.ndarray, top_m: int = 3, sigma: float = 1.0) -> np.ndarray:
    """Cuantiza descriptores locales en un histograma de palabras visuales.
    
    Utiliza asignación suave (soft-assignment) con pesos gaussianos para asignar
    cada descriptor a los top_m centroides más cercanos.
    
    Args:
        descriptors: Matriz de descriptores locales (n_desc, dim)
        centroids: Centroides del codebook (k, dim)
        top_m: Número de centroides más cercanos a considerar
        sigma: Parámetro de escala para la función gaussiana
        
    Returns:
        Histograma normalizado de palabras visuales (k,)
    """
    if descriptors.shape[0] == 0:
        return np.zeros((centroids.shape[0],), dtype=np.float32)
    dists = pairwise_distances(descriptors, centroids, metric="euclidean")
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
    """Calcula la frecuencia de documento (DF) para cada palabra visual.
    
    Args:
        histograms: Lista de histogramas BoW por documento
        
    Returns:
        Vector DF indicando en cuántos documentos aparece cada palabra visual
    """
    df = np.zeros((histograms[0].shape[0],), dtype=np.int32)
    for h in histograms:
        df += (h > 0).astype(np.int32)
    return df


def compute_tfidf(h: np.ndarray, df: np.ndarray, n_docs: int) -> np.ndarray:
    """Calcula la representación TF-IDF normalizada de un histograma.
    
    Args:
        h: Histograma de palabras visuales
        df: Vector de frecuencia de documento
        n_docs: Número total de documentos
        
    Returns:
        Vector TF-IDF normalizado en L2
    """
    idf = np.log((n_docs + 1) / (df + 1)) + 1.0
    tf = np.log1p(h)
    w = tf * idf
    # L2 normalize
    norm = np.linalg.norm(w)
    if norm > 0:
        w = w / norm
    return w.astype(np.float32)


def save_bow_artifacts(out_dir: str, histograms: List[np.ndarray], doc_ids: List[str], df: np.ndarray):
    """Guarda los artefactos del modelo BoW en disco.
    
    Args:
        out_dir: Directorio de salida
        histograms: Lista de histogramas BoW
        doc_ids: Identificadores de documentos
        df: Vector de frecuencia de documento
    """
    import os
    os.makedirs(out_dir, exist_ok=True)
    for i, (doc_id, h) in enumerate(zip(doc_ids, histograms)):
        np.savez_compressed(os.path.join(out_dir, f"bow_{i}.npz"), doc_id=doc_id, hist=h)
    with open(os.path.join(out_dir, "doc_ids.pkl"), "wb") as f:
        pickle.dump(doc_ids, f)
    with open(os.path.join(out_dir, "df.pkl"), "wb") as f:
        pickle.dump(df, f)


def load_bow_artifacts(out_dir: str) -> Tuple[List[str], List[np.ndarray], np.ndarray]:
    """Carga los artefactos del modelo BoW desde disco.
    
    Args:
        out_dir: Directorio con los artefactos guardados
        
    Returns:
        Tupla (doc_ids, histograms, df)
    """
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
