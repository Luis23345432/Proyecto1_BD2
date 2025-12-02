"""
Índice Invertido con TF-IDF
Implementa la estructura básica del índice invertido en memoria
"""

import math
import pickle
from typing import Dict, List, Tuple, Set
from collections import defaultdict, Counter


class InvertedIndex:
    """
    Índice invertido con cálculo de TF-IDF
    
    Estructura:
    {
        'term1': {
            'doc1': {'tf': 3, 'positions': [0, 5, 10]},
            'doc2': {'tf': 1, 'positions': [7]}
        },
        'term2': {...}
    }
    """
    
    def __init__(self):
        # Índice invertido: term -> {doc_id -> {tf, positions}}
        self.index: Dict[str, Dict[str, Dict]] = defaultdict(lambda: defaultdict(dict))
        
        # Metadatos de documentos
        self.doc_norms: Dict[str, float] = {}  # Norma euclidiana de cada documento
        self.doc_lengths: Dict[str, int] = {}   # Longitud en términos de cada documento
        
        # Estadísticas globales
        self.num_documents: int = 0
        self.df: Dict[str, int] = defaultdict(int)  # Document frequency por término
        self.avg_doc_length: float = 0.0
    
    def add_document(self, doc_id: str, tokens: List[str]):
        """
        Agrega un documento al índice
        
        Args:
            doc_id: ID único del documento
            tokens: Lista de tokens preprocesados del documento
        """
        self.num_documents += 1
        self.doc_lengths[doc_id] = len(tokens)
        
        # Contar frecuencias de términos (TF)
        term_freq = Counter(tokens)
        
        # Actualizar índice invertido
        for position, term in enumerate(tokens):
            if doc_id not in self.index[term]:
                # Primera vez que este término aparece en este documento
                self.df[term] += 1
                self.index[term][doc_id] = {
                    'tf': 0,
                    'positions': []
                }
            
            self.index[term][doc_id]['tf'] = term_freq[term]
            self.index[term][doc_id]['positions'].append(position)
        
        # Actualizar longitud promedio
        self._update_avg_length()
    
    def _update_avg_length(self):
        """Actualiza la longitud promedio de documentos"""
        if self.num_documents > 0:
            total_length = sum(self.doc_lengths.values())
            self.avg_doc_length = total_length / self.num_documents
    
    def calculate_tf_idf(self):
        """
        Calcula los pesos TF-IDF para todos los términos en todos los documentos
        y las normas de los documentos
        """
        print(f"📊 Calculando TF-IDF para {self.num_documents} documentos...")
        
        # Para cada documento, calcular su vector TF-IDF y su norma
        for doc_id in self.doc_lengths.keys():
            tfidf_vector = []
            
            # Calcular TF-IDF para cada término en el documento
            for term, docs in self.index.items():
                if doc_id in docs:
                    tf = docs[doc_id]['tf']
                    
                    # TF: log(1 + tf)
                    tf_weight = math.log10(1 + tf) if tf > 0 else 0
                    
                    # IDF: log(N / df)
                    idf_weight = math.log10(self.num_documents / self.df[term])
                    
                    # TF-IDF
                    tfidf = tf_weight * idf_weight
                    
                    # Guardar en el índice
                    docs[doc_id]['tfidf'] = tfidf
                    tfidf_vector.append(tfidf)
            
            # Calcular norma euclidiana del documento
            norm = math.sqrt(sum(w ** 2 for w in tfidf_vector))
            self.doc_norms[doc_id] = norm if norm > 0 else 1.0
        
        print(f"✅ TF-IDF calculado. Normas de documentos almacenadas.")
    
    def get_posting_list(self, term: str) -> Dict[str, Dict]:
        """
        Obtiene la posting list de un término
        
        Args:
            term: Término a buscar
            
        Returns:
            Diccionario {doc_id -> {tf, tfidf, positions}}
        """
        return self.index.get(term, {})
    
    def get_document_vector(self, doc_id: str) -> Dict[str, float]:
        """
        Obtiene el vector TF-IDF de un documento
        
        Args:
            doc_id: ID del documento
            
        Returns:
            Diccionario {term -> tfidf_weight}
        """
        vector = {}
        for term, docs in self.index.items():
            if doc_id in docs and 'tfidf' in docs[doc_id]:
                vector[term] = docs[doc_id]['tfidf']
        return vector
    
    def search_boolean(self, query_terms: List[str]) -> Set[str]:
        """
        Búsqueda booleana AND (todos los términos deben aparecer)
        
        Args:
            query_terms: Lista de términos de la consulta
            
        Returns:
            Set de doc_ids que contienen TODOS los términos
        """
        if not query_terms:
            return set()
        
        # Obtener posting lists
        posting_lists = [set(self.get_posting_list(term).keys()) for term in query_terms]
        
        # Intersección (AND)
        result = posting_lists[0]
        for pl in posting_lists[1:]:
            result = result.intersection(pl)
        
        return result
    
    def get_stats(self) -> Dict:
        """Retorna estadísticas del índice"""
        return {
            'num_documents': self.num_documents,
            'num_terms': len(self.index),
            'avg_doc_length': round(self.avg_doc_length, 2),
            'total_postings': sum(len(docs) for docs in self.index.values())
        }
    
    def save(self, filepath: str):
        """Guarda el índice en disco"""
        with open(filepath, 'wb') as f:
            pickle.dump({
                'index': dict(self.index),
                'doc_norms': self.doc_norms,
                'doc_lengths': self.doc_lengths,
                'num_documents': self.num_documents,
                'df': dict(self.df),
                'avg_doc_length': self.avg_doc_length
            }, f)
        print(f"💾 Índice guardado en {filepath}")
    
    @classmethod
    def load(cls, filepath: str) -> 'InvertedIndex':
        """Carga el índice desde disco"""
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        idx = cls()
        idx.index = defaultdict(lambda: defaultdict(dict), data['index'])
        idx.doc_norms = data['doc_norms']
        idx.doc_lengths = data['doc_lengths']
        idx.num_documents = data['num_documents']
        idx.df = defaultdict(int, data['df'])
        idx.avg_doc_length = data['avg_doc_length']
        
        print(f"📂 Índice cargado desde {filepath}")
        return idx


if __name__ == "__main__":
    print("=" * 60)
    print("PRUEBA DEL ÍNDICE INVERTIDO")
    print("=" * 60)
    
    # Crear índice
    index = InvertedIndex()
    
    # Documentos de ejemplo (ya preprocesados)
    documents = {
        'doc1': ['casa', 'grande', 'jardin', 'bonito'],
        'doc2': ['casa', 'pequeña', 'jardin'],
        'doc3': ['grande', 'edificio', 'ciudad'],
        'doc4': ['casa', 'bonito', 'ciudad']
    }
    
    # Agregar documentos
    print("\n📚 Agregando documentos al índice...")
    for doc_id, tokens in documents.items():
        index.add_document(doc_id, tokens)
        print(f"  {doc_id}: {tokens}")
    
    # Calcular TF-IDF
    print("\n🔢 Calculando TF-IDF...")
    index.calculate_tf_idf()
    
    # Mostrar estadísticas
    print("\n📊 Estadísticas del índice:")
    stats = index.get_stats()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Búsqueda booleana
    print("\n🔍 Búsqueda booleana: ['casa', 'jardin']")
    results = index.search_boolean(['casa', 'jardin'])
    print(f"  Documentos encontrados: {results}")
    
    # Posting list de un término
    print("\n📋 Posting list de 'casa':")
    posting = index.get_posting_list('casa')
    for doc_id, data in posting.items():
        print(f"  {doc_id}: TF={data['tf']}, TF-IDF={data.get('tfidf', 0):.4f}")
    
    # Vector de un documento
    print("\n📐 Vector TF-IDF de 'doc1':")
    vector = index.get_document_vector('doc1')
    for term, weight in sorted(vector.items(), key=lambda x: x[1], reverse=True):
        print(f"  {term}: {weight:.4f}")
    
    # Guardar índice
    print("\n💾 Guardando índice...")
    index.save('test_index.pkl')
    
    # Cargar índice
    print("\n📂 Cargando índice...")
    loaded_index = InvertedIndex.load('test_index.pkl')
    print(f"  Documentos cargados: {loaded_index.num_documents}")
    
    print("\n" + "=" * 60)