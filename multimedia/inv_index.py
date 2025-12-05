"""Módulo para la construcción y búsqueda en índices invertidos multimedia.

Implementa un índice invertido para consultas KNN eficientes sobre representaciones
BoW. Cada palabra visual mantiene una lista de posting (documento, peso TF-IDF)
para acelerar la búsqueda por similitud coseno.
"""

import logging
import os
import pickle
from typing import Dict, List, Tuple

import numpy as np


logger = logging.getLogger(__name__)


def build_inverted_index(doc_ids: List[str], histograms: List[np.ndarray], out_dir: str):
    """Construye un índice invertido a partir de histogramas BoW.
    
    Calcula pesos TF-IDF normalizados y crea listas de posting para cada
    palabra visual, permitiendo búsquedas KNN eficientes.
    
    Args:
        doc_ids: Identificadores de documentos
        histograms: Lista de histogramas BoW por documento
        out_dir: Directorio donde guardar el índice
    """
    os.makedirs(out_dir, exist_ok=True)
    n_docs = len(doc_ids)
    k = histograms[0].shape[0]
    postings: Dict[int, List[Tuple[int, float]]] = {i: [] for i in range(k)}
    df = np.zeros((k,), dtype=np.int32)
    for d_i, h in enumerate(histograms):
        active = np.where(h > 0)[0]
        df[active] += 1
    idf = np.log((n_docs + 1) / (df + 1)) + 1.0
    for d_i, h in enumerate(histograms):
        w = h * idf
        norm = np.linalg.norm(w)
        if norm > 0:
            w = w / norm
        active = np.where(w > 0)[0]
        for cw in active:
            postings[cw].append((d_i, float(w[cw])))
    with open(os.path.join(out_dir, "doc_ids.pkl"), "wb") as f:
        pickle.dump(doc_ids, f)
    with open(os.path.join(out_dir, "idf.pkl"), "wb") as f:
        pickle.dump(idf, f)
    term_to_block = {}
    for cw, plist in postings.items():
        path = os.path.join(out_dir, f"cw_{cw}.pkl")
        with open(path, "wb") as f:
            pickle.dump(plist, f)
        term_to_block[cw] = path
    with open(os.path.join(out_dir, "term_to_block.pkl"), "wb") as f:
        pickle.dump(term_to_block, f)


def search_inverted(query_hist: np.ndarray, index_dir: str, top_k: int = 10) -> List[Tuple[str, float]]:
    """Busca los K documentos más similares usando el índice invertido.
    
    Calcula la similitud coseno entre la consulta y los documentos indexados
    usando acceso eficiente mediante listas de posting.
    
    Args:
        query_hist: Histograma BoW de la consulta
        index_dir: Directorio del índice invertido
        top_k: Número de resultados a retornar
        
    Returns:
        Lista de tuplas (doc_id, score) ordenadas por similitud descendente
    """
    with open(os.path.join(index_dir, "doc_ids.pkl"), "rb") as f:
        doc_ids = pickle.load(f)
    with open(os.path.join(index_dir, "idf.pkl"), "rb") as f:
        idf = pickle.load(f)
    with open(os.path.join(index_dir, "term_to_block.pkl"), "rb") as f:
        t2b = pickle.load(f)
    if idf.shape[0] != query_hist.shape[0]:
        raise ValueError(
            f"Codebook dimensionality mismatch: query_hist has {query_hist.shape[0]} bins "
            f"but index was built with {idf.shape[0]} bins. Rebuild BoW and inverted index "
            f"using the same codebook size."
        )
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
            plist = pickle.load(f)
        wq_cw = float(wq[cw])
        for d_i, w_doc in plist:
            scores[d_i] = scores.get(d_i, 0.0) + wq_cw * w_doc
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
