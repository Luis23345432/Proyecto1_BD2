"""
Búsqueda con Similitud de Coseno
Implementa ranking de documentos usando TF-IDF y similitud coseno
"""

import math
from typing import List, Tuple, Dict
from collections import Counter


class CosineSearch:
    """
    Motor de búsqueda basado en similitud de coseno
    """
    
    def __init__(self, inverted_index):
        """
        Args:
            inverted_index: Instancia de InvertedIndex con TF-IDF calculado
        """
        self.index = inverted_index
    
    def search(self, query_tokens: List[str], top_k: int = 10) -> List[Tuple[str, float]]:
        """
        Busca documentos similares a la consulta usando similitud de coseno
        
        Args:
            query_tokens: Lista de tokens de la consulta (ya preprocesados)
            top_k: Número de documentos a retornar
            
        Returns:
            Lista de tuplas (doc_id, score) ordenadas por score descendente
        """
        if not query_tokens:
            return []
        
        # 1. Calcular vector TF-IDF de la consulta
        query_vector = self._compute_query_tfidf(query_tokens)
        
        # 2. Calcular norma de la consulta
        query_norm = math.sqrt(sum(w ** 2 for w in query_vector.values()))
        if query_norm == 0:
            return []
        
        # 3. Encontrar documentos candidatos (que contengan al menos un término)
        candidate_docs = set()
        for term in query_vector.keys():
            posting_list = self.index.get_posting_list(term)
            candidate_docs.update(posting_list.keys())
        
        print(f"🔍 Consulta: {query_tokens}")
        print(f"📊 Documentos candidatos: {len(candidate_docs)}")
        
        # 4. Calcular similitud coseno para cada documento candidato
        scores = []
        for doc_id in candidate_docs:
            score = self._cosine_similarity(query_vector, doc_id, query_norm)
            if score > 0:
                scores.append((doc_id, score))
        
        # 5. Ordenar por score descendente y retornar top-k
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]
    
    def _compute_query_tfidf(self, query_tokens: List[str]) -> Dict[str, float]:
        """
        Calcula el vector TF-IDF para la consulta
        
        Args:
            query_tokens: Tokens de la consulta
            
        Returns:
            Diccionario {term -> tfidf_weight}
        """
        # Contar frecuencia de términos en la consulta
        term_freq = Counter(query_tokens)
        
        query_vector = {}
        for term, tf in term_freq.items():
            # TF: log(1 + tf)
            tf_weight = math.log10(1 + tf)
            
            # IDF: log(N / df)
            df = self.index.df.get(term, 0)
            if df > 0:
                idf_weight = math.log10(self.index.num_documents / df)
            else:
                idf_weight = 0
            
            # TF-IDF
            query_vector[term] = tf_weight * idf_weight
        
        return query_vector
    
    def _cosine_similarity(self, query_vector: Dict[str, float], doc_id: str, query_norm: float) -> float:
        """
        Calcula la similitud coseno entre la consulta y un documento
        
        Formula: cos(q, d) = (q · d) / (||q|| * ||d||)
        
        Args:
            query_vector: Vector TF-IDF de la consulta
            doc_id: ID del documento
            query_norm: Norma euclidiana de la consulta
            
        Returns:
            Score de similitud [0, 1]
        """
        # Obtener norma del documento
        doc_norm = self.index.doc_norms.get(doc_id, 1.0)
        
        # Calcular producto punto (dot product)
        dot_product = 0.0
        for term, query_weight in query_vector.items():
            posting_list = self.index.get_posting_list(term)
            if doc_id in posting_list:
                doc_weight = posting_list[doc_id].get('tfidf', 0.0)
                dot_product += query_weight * doc_weight
        
        # Similitud coseno
        if doc_norm == 0:
            return 0.0
        
        similarity = dot_product / (query_norm * doc_norm)
        return similarity


if __name__ == "__main__":
    # Import relativo cuando se ejecuta directamente
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    from inverted_index import InvertedIndex
    
    print("=" * 60)
    print("PRUEBA DE BÚSQUEDA CON SIMILITUD DE COSENO")
    print("=" * 60)
    
    # Crear índice con documentos de ejemplo
    index = InvertedIndex()
    
    documents = {
        'song1': ['amor', 'corazon', 'sentimiento', 'vida'],
        'song2': ['amor', 'pasion', 'fuego', 'corazon'],
        'song3': ['ciudad', 'noche', 'luz', 'calle'],
        'song4': ['amor', 'dolor', 'lagrimas', 'vida'],
        'song5': ['corazon', 'roto', 'dolor', 'tristeza']
    }
    
    print("\n📚 Documentos de ejemplo:")
    for doc_id, tokens in documents.items():
        index.add_document(doc_id, tokens)
        print(f"  {doc_id}: {' '.join(tokens)}")
    
    # Calcular TF-IDF
    index.calculate_tf_idf()
    
    # Crear motor de búsqueda
    searcher = CosineSearch(index)
    
    # Consulta 1
    print("\n" + "=" * 60)
    query1 = ['amor', 'corazon']
    print(f"🔍 Consulta: {query1}")
    results = searcher.search(query1, top_k=3)
    print("\n📊 Top-3 Resultados:")
    for i, (doc_id, score) in enumerate(results, 1):
        print(f"  {i}. {doc_id} (score: {score:.4f})")
        print(f"     Contenido: {' '.join(documents[doc_id])}")
    
    # Consulta 2
    print("\n" + "=" * 60)
    query2 = ['dolor', 'tristeza']
    print(f"🔍 Consulta: {query2}")
    results = searcher.search(query2, top_k=3)
    print("\n📊 Top-3 Resultados:")
    for i, (doc_id, score) in enumerate(results, 1):
        print(f"  {i}. {doc_id} (score: {score:.4f})")
        print(f"     Contenido: {' '.join(documents[doc_id])}")
    
    # Consulta 3
    print("\n" + "=" * 60)
    query3 = ['ciudad', 'noche']
    print(f"🔍 Consulta: {query3}")
    results = searcher.search(query3, top_k=3)
    print("\n📊 Top-3 Resultados:")
    for i, (doc_id, score) in enumerate(results, 1):
        print(f"  {i}. {doc_id} (score: {score:.4f})")
        print(f"     Contenido: {' '.join(documents[doc_id])}")
    
    print("\n" + "=" * 60)