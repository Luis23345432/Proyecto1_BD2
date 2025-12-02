"""
Búsqueda con Similitud de Coseno
Implementa ranking de documentos usando TF-IDF y similitud coseno
Optimizado para trabajar con índice invertido en disco (SPIMI)
"""

import math
import pickle
import os
from typing import List, Tuple, Dict
from collections import Counter
import heapq


class CosineSearch:
    """
    Motor de búsqueda basado en similitud de coseno
    Versión optimizada para índices en memoria secundaria
    """
    
    def __init__(self, index_dir: str = 'data/spimi_blocks', use_memory_index=None):
        """
        Args:
            index_dir: Directorio donde está el índice SPIMI
            use_memory_index: Si se proporciona, usa InvertedIndex en RAM (para compatibilidad)
        """
        self.use_memory = use_memory_index is not None
        
        if self.use_memory:
            # Modo RAM (tu implementación original)
            self.index = use_memory_index
            print("🔧 Modo: Índice en RAM")
        else:
            # Modo DISCO (nueva implementación)
            self.index_dir = index_dir
            self._load_metadata()
            print(f"🔧 Modo: Índice en disco ({index_dir})")
            print(f"📊 Total documentos: {self.num_documents}")
            print(f"📊 Total términos únicos: {len(self.idf_scores)}")
    
    def _load_metadata(self):
        """Carga metadatos necesarios para búsqueda desde disco"""
        # Cargar normas de documentos
        norms_path = os.path.join(self.index_dir, 'doc_norms.pkl')
        with open(norms_path, 'rb') as f:
            self.doc_norms = pickle.load(f)
        
        # Cargar IDs de documentos
        ids_path = os.path.join(self.index_dir, 'doc_ids.pkl')
        with open(ids_path, 'rb') as f:
            self.doc_ids = pickle.load(f)
        
        # Cargar scores IDF
        idf_path = os.path.join(self.index_dir, 'idf_scores.pkl')
        with open(idf_path, 'rb') as f:
            self.idf_scores = pickle.load(f)
        
        # Cargar mapeo de términos a bloques
        mapping_path = os.path.join(self.index_dir, 'term_to_block.pkl')
        with open(mapping_path, 'rb') as f:
            self.term_to_block = pickle.load(f)
        
        self.num_documents = len(self.doc_norms)
    
    def _load_posting_list_from_disk(self, term: str) -> Dict[str, float]:
        """
        Carga la posting list de un término desde disco.
        OPTIMIZACIÓN: Solo carga el bloque necesario, no todo el índice.
        
        Args:
            term: Término a buscar
            
        Returns:
            Diccionario {doc_id: tfidf_weight}
        """
        # Verificar si el término existe
        block_file = self.term_to_block.get(term)
        if not block_file:
            return {}
        
        # Cargar solo el bloque que contiene este término
        block_path = os.path.join(self.index_dir, block_file)
        with open(block_path, 'rb') as f:
            block_index = pickle.load(f)
        
        # Retornar las postings del término (ya normalizadas)
        return block_index.get(term, {})
    
    def search(self, query_tokens: List[str], top_k: int = 10, k: int = None) -> List[Tuple[str, float]]:
        """
        Busca documentos similares a la consulta usando similitud de coseno
        
        Args:
            query_tokens: Lista de tokens de la consulta (ya preprocesados)
            top_k: Número de documentos a retornar (alias de k)
            k: Número de documentos a retornar (para compatibilidad)
            
        Returns:
            Lista de tuplas (doc_id, score) ordenadas por score descendente
        """
        # Compatibilidad: aceptar tanto k como top_k
        if k is not None:
            top_k = k
        if not query_tokens:
            return []
        
        # 1. Calcular vector TF-IDF de la consulta
        query_vector = self._compute_query_tfidf(query_tokens)
        
        # 2. Calcular norma de la consulta
        query_norm = math.sqrt(sum(w ** 2 for w in query_vector.values()))
        if query_norm == 0:
            return []
        
        # Normalizar vector de consulta
        query_vector = {term: weight/query_norm for term, weight in query_vector.items()}
        
        # 3. Calcular scores de similitud
        if self.use_memory:
            # Versión RAM (tu código original)
            scores = self._search_memory(query_vector, top_k)
        else:
            # Versión DISCO (optimizada)
            scores = self._search_disk(query_vector, top_k)
        
        return scores
    
    def _search_memory(self, query_vector: Dict[str, float], top_k: int) -> List[Tuple[str, float]]:
        """
        Búsqueda usando índice en RAM (tu implementación original)
        """
        # Encontrar documentos candidatos
        candidate_docs = set()
        for term in query_vector.keys():
            posting_list = self.index.get_posting_list(term)
            candidate_docs.update(posting_list.keys())
        
        print(f"📊 Documentos candidatos: {len(candidate_docs)}")
        
        # Calcular similitud para cada candidato
        scores = []
        for doc_id in candidate_docs:
            score = self._cosine_similarity_memory(query_vector, doc_id)
            if score > 0:
                scores.append((doc_id, score))
        
        # Ordenar y retornar top-k
        scores.sort(key=lambda x: x[1], reverse=True)
        return scores[:top_k]
    
    def _search_disk(self, query_vector: Dict[str, float], top_k: int) -> List[Tuple[str, float]]:
        """
        Búsqueda usando índice en DISCO (optimizada).
        CLAVE: No carga todo el índice en RAM, solo los términos de la consulta.
        """
        # Acumular scores por documento
        doc_scores = {}
        
        # Para cada término de la consulta
        for term, query_weight in query_vector.items():
            # Cargar posting list solo para este término (desde disco)
            posting_list = self._load_posting_list_from_disk(term)
            
            # Acumular contribución al score
            for doc_id, doc_weight in posting_list.items():
                if doc_id not in doc_scores:
                    doc_scores[doc_id] = 0.0
                # Los pesos ya están normalizados, solo multiplicar
                doc_scores[doc_id] += query_weight * doc_weight
        
        if not doc_scores:
            print("❌ No se encontraron documentos candidatos")
            return []
        
        print(f"📊 Documentos candidatos: {len(doc_scores)}")
        
        # Usar heap para obtener top-k eficientemente
        # OPTIMIZACIÓN: O(n log k) en lugar de O(n log n)
        top_results = heapq.nlargest(top_k, doc_scores.items(), key=lambda x: x[1])
        
        # Convertir doc_id numérico a string original
        results = []
        for doc_id, score in top_results:
            original_id = self.doc_ids.get(doc_id, str(doc_id))
            results.append((original_id, score))
        
        return results
    
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
            
            # IDF: obtener desde el índice
            if self.use_memory:
                df = self.index.df.get(term, 0)
                if df > 0:
                    idf_weight = math.log10(self.index.num_documents / df)
                else:
                    idf_weight = 0
            else:
                idf_weight = self.idf_scores.get(term, 0)
            
            # TF-IDF
            query_vector[term] = tf_weight * idf_weight
        
        return query_vector
    
    def _cosine_similarity_memory(self, query_vector: Dict[str, float], doc_id: str) -> float:
        """
        Calcula similitud coseno para índice en RAM (versión original)
        """
        # Obtener norma del documento
        doc_norm = self.index.doc_norms.get(doc_id, 1.0)
        
        # Calcular producto punto
        dot_product = 0.0
        for term, query_weight in query_vector.items():
            posting_list = self.index.get_posting_list(term)
            if doc_id in posting_list:
                doc_weight = posting_list[doc_id].get('tfidf', 0.0)
                dot_product += query_weight * doc_weight
        
        # Similitud coseno
        if doc_norm == 0:
            return 0.0
        
        # Como query_vector ya está normalizado, solo dividir por doc_norm
        similarity = dot_product / doc_norm
        return similarity


def search_documents(query: str, k: int = 10, 
                    index_dir: str = 'data/spimi_blocks',
                    preprocessor=None) -> List[Dict]:
    """
    Función auxiliar para búsqueda completa con preprocesamiento.
    
    Args:
        query: Consulta en lenguaje natural
        k: Número de resultados
        index_dir: Directorio del índice
        preprocessor: Instancia de TextPreprocessor (opcional)
        
    Returns:
        Lista de resultados con detalles
    """
    # Preprocesar la consulta
    if preprocessor is None:
        from preprocessor import TextPreprocessor
        preprocessor = TextPreprocessor()
    
    query_tokens = preprocessor.preprocess(query)
    
    # Ejecutar búsqueda
    searcher = CosineSearch(index_dir=index_dir)
    results = searcher.search(query_tokens, top_k=k)
    
    # Formatear resultados
    detailed_results = []
    for doc_id, score in results:
        detailed_results.append({
            'doc_id': doc_id,
            'score': score,
            'similarity_percentage': round(score * 100, 2)
        })
    
    return detailed_results


if __name__ == "__main__":
    import sys
    import os
    sys.path.insert(0, os.path.dirname(__file__))
    
    print("=" * 80)
    print("PRUEBA DE BÚSQUEDA CON SIMILITUD DE COSENO")
    print("=" * 80)
    
    # Opción 1: Probar con índice en RAM (tu versión original)
    print("\n🧪 PRUEBA 1: Índice en RAM")
    print("-" * 80)
    
    from inverted_index import InvertedIndex
    
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
    
    # Crear motor de búsqueda (modo RAM)
    searcher = CosineSearch(use_memory_index=index)
    
    # Consulta 1
    print("\n" + "=" * 80)
    query1 = ['amor', 'corazon']
    print(f"🔍 Consulta: {query1}")
    results = searcher.search(query1, top_k=3)
    print("\n📊 Top-3 Resultados:")
    for i, (doc_id, score) in enumerate(results, 1):
        print(f"  {i}. {doc_id} (score: {score:.4f}, {score*100:.2f}%)")
        print(f"     Contenido: {' '.join(documents[doc_id])}")
    
    # Consulta 2
    print("\n" + "=" * 80)
    query2 = ['dolor', 'tristeza']
    print(f"🔍 Consulta: {query2}")
    results = searcher.search(query2, top_k=3)
    print("\n📊 Top-3 Resultados:")
    for i, (doc_id, score) in enumerate(results, 1):
        print(f"  {i}. {doc_id} (score: {score:.4f}, {score*100:.2f}%)")
        print(f"     Contenido: {' '.join(documents[doc_id])}")
    
    # Consulta 3
    print("\n" + "=" * 80)
    query3 = ['ciudad', 'noche']
    print(f"🔍 Consulta: {query3}")
    results = searcher.search(query3, top_k=3)
    print("\n📊 Top-3 Resultados:")
    for i, (doc_id, score) in enumerate(results, 1):
        print(f"  {i}. {doc_id} (score: {score:.4f}, {score*100:.2f}%)")
        print(f"     Contenido: {' '.join(documents[doc_id])}")
    
    print("\n" + "=" * 80)
    print("✅ Prueba con índice en RAM completada")
    
    # Opción 2: Probar con índice en disco (si existe)
    print("\n" + "=" * 80)
    print("🧪 PRUEBA 2: Índice en DISCO (SPIMI)")
    print("-" * 80)
    
    index_dir = 'data/spimi_blocks'
    if os.path.exists(index_dir):
        print(f"✓ Directorio encontrado: {index_dir}")
        
        # Verificar archivos necesarios
        required_files = ['doc_norms.pkl', 'doc_ids.pkl', 'idf_scores.pkl', 'term_to_block.pkl']
        missing = [f for f in required_files if not os.path.exists(os.path.join(index_dir, f))]
        
        if missing:
            print(f"⚠️  Archivos faltantes: {missing}")
            print("   Ejecuta SPIMI completo primero para generar estos archivos")
        else:
            print("✓ Todos los archivos necesarios están presentes")
            print("\nEjecutando búsqueda en índice de disco...")
            
            try:
                # Usar la función auxiliar
                results = search_documents("amor corazon", k=5, index_dir=index_dir)
                
                print("\n📊 Resultados:")
                for i, result in enumerate(results, 1):
                    print(f"  {i}. Doc: {result['doc_id']}")
                    print(f"     Score: {result['score']:.4f} ({result['similarity_percentage']}%)")
                
                print("\n✅ Búsqueda en disco completada exitosamente")
                
            except Exception as e:
                print(f"❌ Error al buscar en disco: {str(e)}")
                import traceback
                traceback.print_exc()
    else:
        print(f"⚠️  Directorio no encontrado: {index_dir}")
        print("   Ejecuta SPIMI primero para generar el índice")
    
    print("\n" + "=" * 80)