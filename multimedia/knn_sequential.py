"""Módulo para búsqueda KNN secuencial sobre representaciones BoW.

Implementa búsqueda de vecinos más cercanos mediante exploración lineal
de todos los documentos, calculando similitud coseno con ponderación TF-IDF.
Sirve como baseline para comparar con métodos indexados.
"""

import os
import pickle
from typing import List, Tuple

import numpy as np
from .bow import compute_df


def load_bow(out_dir: str):
    """Carga los histogramas BoW guardados en disco.
    
    Args:
        out_dir: Directorio con los artefactos BoW
        
    Returns:
        Tupla (doc_ids, histogramas)
    """
    with open(os.path.join(out_dir, "doc_ids.pkl"), "rb") as f:
        doc_ids = pickle.load(f)
    hists = []
    for i in range(len(doc_ids)):
        obj = np.load(os.path.join(out_dir, f"bow_{i}.npz"))
        hists.append(obj["hist"])

    return doc_ids, hists


def tfidf_normalize(h: np.ndarray, df: np.ndarray, n_docs: int) -> np.ndarray:
    """Normaliza un histograma usando esquema TF-IDF.
    
    Args:
        h: Histograma de palabras visuales
        df: Vector de frecuencia de documento
        n_docs: Número total de documentos
        
    Returns:
        Vector TF-IDF normalizado en L2
    """
    if df.shape[0] != h.shape[0]:
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
    """Busca los K documentos más similares mediante exploración secuencial.
    
    Calcula similitud coseno entre la consulta y todos los documentos,
    retornando los top-K más similares.
    
    Args:
        query_hist: Histograma BoW de la consulta
        bow_dir: Directorio con los artefactos BoW
        top_k: Número de resultados a retornar
        
    Returns:
        Lista de tuplas (doc_id, score) ordenadas por similitud descendente
    """
    doc_ids, hists = load_bow(bow_dir)
    n_docs = len(doc_ids)

    k = query_hist.shape[0]
    hists = [h if h.shape[0] == k else (h[:k] if h.shape[0] > k else np.pad(h, (0, k - h.shape[0]), constant_values=0.0)) for h in hists]

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
