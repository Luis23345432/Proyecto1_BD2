"""
SPIMI (Single-Pass In-Memory Indexing)
Construcción de índice invertido en memoria secundaria para grandes volúmenes
"""

import os
import pickle
import math
import heapq
from typing import Dict, List, Tuple, Iterator
from collections import defaultdict, Counter


class SPIMIIndexer:
    """
    Implementa el algoritmo SPIMI para construir índices invertidos
    en bloques cuando la RAM es limitada
    """
    
    def __init__(self, output_dir: str, block_size_mb: int = 100):
        """
        Args:
            output_dir: Directorio donde se guardarán los bloques
            block_size_mb: Tamaño máximo de cada bloque en MB
        """
        self.output_dir = output_dir
        self.block_size_mb = block_size_mb
        self.block_size_bytes = block_size_mb * 1024 * 1024
        
        os.makedirs(self.output_dir, exist_ok=True)
        
        # Estadísticas globales
        self.num_documents = 0
        self.num_blocks = 0
        self.doc_lengths = {}
        self.df = defaultdict(int)  # Document frequency global
    
    def build_index(self, document_iterator: Iterator[Tuple[str, List[str]]]) -> str:
        """
        Construye el índice usando SPIMI
        
        Args:
            document_iterator: Iterador que genera tuplas (doc_id, tokens)
            
        Returns:
            Ruta del índice final merged
        """
        print("\n" + "=" * 60)
        print("🔨 INICIANDO CONSTRUCCIÓN SPIMI")
        print("=" * 60)
        
        # Fase 1: Construir bloques locales
        block_files = self._build_blocks(document_iterator)
        
        # Fase 2: Merge de bloques
        final_index_path = self._merge_blocks(block_files)
        
        # Fase 3: Calcular TF-IDF y normas
        self._calculate_tfidf(final_index_path)
        
        print("\n✅ Índice SPIMI construido exitosamente")
        print("=" * 60)
        
        return final_index_path
    
    def _build_blocks(self, document_iterator: Iterator[Tuple[str, List[str]]]) -> List[str]:
        """
        Fase 1: Construir bloques locales en memoria
        """
        print("\n📦 Fase 1: Construyendo bloques locales...")
        
        block_files = []
        current_block = defaultdict(lambda: defaultdict(list))
        current_size = 0
        docs_in_block = 0
        
        for doc_id, tokens in document_iterator:
            self.num_documents += 1
            self.doc_lengths[doc_id] = len(tokens)
            
            # Agregar términos al bloque actual
            term_freq = Counter(tokens)
            
            for term, tf in term_freq.items():
                # Primera vez que vemos este término en este doc en este bloque
                if doc_id not in current_block[term]:
                    self.df[term] += 1
                
                current_block[term][doc_id] = {
                    'tf': tf,
                    'positions': [i for i, t in enumerate(tokens) if t == term]
                }
                
                # Estimar tamaño (aproximado)
                current_size += len(term) + len(doc_id) + 24  # overhead
            
            docs_in_block += 1
            
            # Si el bloque excede el tamaño máximo, escribirlo a disco
            if current_size >= self.block_size_bytes:
                block_file = self._write_block(current_block, docs_in_block)
                block_files.append(block_file)
                
                # Reset para siguiente bloque
                current_block = defaultdict(lambda: defaultdict(list))
                current_size = 0
                docs_in_block = 0
        
        # Escribir último bloque si tiene contenido
        if current_block:
            block_file = self._write_block(current_block, docs_in_block)
            block_files.append(block_file)
        
        print(f"✅ {len(block_files)} bloques creados")
        print(f"📊 Total de documentos: {self.num_documents}")
        
        return block_files
    
    def _write_block(self, block: Dict, num_docs: int) -> str:
        """
        Escribe un bloque a disco (ordenado por término)
        """
        self.num_blocks += 1
        block_file = os.path.join(self.output_dir, f"block_{self.num_blocks}.pkl")
        
        # Ordenar términos alfabéticamente
        sorted_terms = sorted(block.keys())
        sorted_block = {term: dict(block[term]) for term in sorted_terms}
        
        with open(block_file, 'wb') as f:
            pickle.dump(sorted_block, f)
        
        print(f"  💾 Bloque {self.num_blocks} guardado: {num_docs} docs, {len(sorted_terms)} términos")
        
        return block_file
    
    def _merge_blocks(self, block_files: List[str]) -> str:
        """
        Fase 2: Merge de bloques usando B buffers (multi-way merge)
        Usa un heap para encontrar el término mínimo eficientemente
        """
        print(f"\n🔀 Fase 2: Merging {len(block_files)} bloques...")
        
        if len(block_files) == 1:
            # Un solo bloque, no hay nada que mergear
            final_path = os.path.join(self.output_dir, "final_index.pkl")
            os.rename(block_files[0], final_path)
            return final_path
        
        # Abrir todos los bloques
        block_iterators = []
        for i, block_file in enumerate(block_files):
            with open(block_file, 'rb') as f:
                block = pickle.load(f)
                # Crear iterador para este bloque
                block_iter = iter(sorted(block.items()))
                try:
                    first_term, first_posting = next(block_iter)
                    # (term, posting_list, block_id, iterator)
                    heapq.heappush(block_iterators, (first_term, first_posting, i, block_iter))
                except StopIteration:
                    pass
        
        # Merged index
        merged_index = {}
        last_term = None
        
        while block_iterators:
            # Obtener el término mínimo
            term, posting_list, block_id, block_iter = heapq.heappop(block_iterators)
            
            # Si es un término nuevo
            if term != last_term:
                if last_term is not None:
                    # Guardar término anterior
                    pass
                merged_index[term] = defaultdict(dict)
                last_term = term
            
            # Merge posting lists
            for doc_id, data in posting_list.items():
                merged_index[term][doc_id] = data
            
            # Avanzar en el bloque
            try:
                next_term, next_posting = next(block_iter)
                heapq.heappush(block_iterators, (next_term, next_posting, block_id, block_iter))
            except StopIteration:
                # Este bloque se terminó
                pass
        
        # Guardar índice merged
        final_path = os.path.join(self.output_dir, "final_index.pkl")
        with open(final_path, 'wb') as f:
            pickle.dump(dict(merged_index), f)
        
        print(f"✅ Merge completado: {len(merged_index)} términos únicos")
        
        # Eliminar bloques temporales
        for block_file in block_files:
            if os.path.exists(block_file):
                os.remove(block_file)
        
        return final_path
    
    def _calculate_tfidf(self, index_path: str):
        """
        Fase 3: Calcular TF-IDF y normas de documentos
        """
        print(f"\n🔢 Fase 3: Calculando TF-IDF...")
        
        # Cargar índice
        with open(index_path, 'rb') as f:
            index = pickle.load(f)
        
        # Calcular TF-IDF para cada término en cada documento
        doc_norms = defaultdict(float)
        
        for term, postings in index.items():
            df = len(postings)
            idf = math.log10(self.num_documents / df) if df > 0 else 0
            
            for doc_id, data in postings.items():
                tf = data['tf']
                tf_weight = math.log10(1 + tf) if tf > 0 else 0
                tfidf = tf_weight * idf
                
                # Guardar TF-IDF
                data['tfidf'] = tfidf
                
                # Acumular para norma
                doc_norms[doc_id] += tfidf ** 2
        
        # Calcular normas finales (raíz cuadrada)
        doc_norms = {doc_id: math.sqrt(norm) if norm > 0 else 1.0 
                     for doc_id, norm in doc_norms.items()}
        
        # Guardar índice con TF-IDF y metadatos
        final_data = {
            'index': index,
            'doc_norms': doc_norms,
            'doc_lengths': self.doc_lengths,
            'num_documents': self.num_documents,
            'df': dict(self.df)
        }
        
        with open(index_path, 'wb') as f:
            pickle.dump(final_data, f)
        
        print(f"✅ TF-IDF calculado para {len(doc_norms)} documentos")


def load_spimi_index(index_path: str):
    """
    Carga un índice construido con SPIMI
    
    Returns:
        Tupla (index, doc_norms, doc_lengths, num_documents, df)
    """
    with open(index_path, 'rb') as f:
        data = pickle.load(f)
    
    return (
        data['index'],
        data['doc_norms'],
        data['doc_lengths'],
        data['num_documents'],
        data['df']
    )


if __name__ == "__main__":
    print("=" * 60)
    print("PRUEBA DE SPIMI")
    print("=" * 60)
    
    # Simular un dataset grande con generador
    def document_generator():
        """Genera documentos de ejemplo"""
        lyrics = [
            "amor corazon sentimiento vida",
            "amor pasion fuego corazon",
            "ciudad noche luz calle",
            "amor dolor lagrimas vida",
            "corazon roto dolor tristeza",
            "baile fiesta alegria musica",
            "amor eterno siempre juntos",
            "ciudad gris lluvia melancolia"
        ]
        
        # Simular 1000 documentos
        for i in range(1000):
            doc_id = f"song_{i}"
            tokens = lyrics[i % len(lyrics)].split()
            yield doc_id, tokens
    
    # Construir índice con SPIMI
    indexer = SPIMIIndexer(output_dir='data/spimi_blocks', block_size_mb=1)
    index_path = indexer.build_index(document_generator())
    
    # Cargar índice
    print("\n📂 Cargando índice construido...")
    index, doc_norms, doc_lengths, num_docs, df = load_spimi_index(index_path)
    
    print(f"\n📊 Estadísticas finales:")
    print(f"  • Documentos: {num_docs}")
    print(f"  • Términos únicos: {len(index)}")
    print(f"  • Término más frecuente: {max(df.items(), key=lambda x: x[1])}")
    
    # Ejemplo de búsqueda en término
    term = 'amor'
    if term in index:
        print(f"\n🔍 Posting list de '{term}':")
        print(f"  • Aparece en {len(index[term])} documentos")
        for doc_id, data in list(index[term].items())[:5]:
            print(f"    - {doc_id}: TF={data['tf']}, TF-IDF={data['tfidf']:.4f}")
    
    print("\n" + "=" * 60)