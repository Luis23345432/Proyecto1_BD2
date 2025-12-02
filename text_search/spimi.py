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
        self.doc_ids = {}  # Mapeo de ID numérico a ID original
        self.df = defaultdict(int)  # Document frequency global
        
        # Para construcción incremental
        self.merged_index = None
    
    def _cleanup_old_files(self):
        """Limpia archivos de construcciones anteriores"""
        if not os.path.exists(self.output_dir):
            return
        
        print("🧹 Limpiando archivos anteriores...")
        files_to_remove = [
            'final_index.pkl',
            'doc_norms.pkl',
            'doc_ids.pkl',
            'idf_scores.pkl',
            'term_to_block.pkl',
            'index_info.pkl'
        ]
        
        # Eliminar archivos específicos
        for filename in files_to_remove:
            filepath = os.path.join(self.output_dir, filename)
            if os.path.exists(filepath):
                os.remove(filepath)
        
        # Eliminar bloques antiguos
        for filename in os.listdir(self.output_dir):
            if filename.startswith('block_') and filename.endswith('.pkl'):
                os.remove(os.path.join(self.output_dir, filename))
    
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
        
        # Limpiar archivos antiguos
        self._cleanup_old_files()
        
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
            # Guardar mapeo de doc_id
            self.doc_ids[self.num_documents] = doc_id
            
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
                current_size += len(term) + len(str(doc_id)) + 24  # overhead
            
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
        
        final_path = os.path.join(self.output_dir, "final_index.pkl")
        
        if len(block_files) == 1:
            # Un solo bloque, no hay nada que mergear
            # Eliminar final_index.pkl si existe
            if os.path.exists(final_path):
                os.remove(final_path)
            
            os.rename(block_files[0], final_path)
            
            # Cargar para tenerlo en memoria
            with open(final_path, 'rb') as f:
                self.merged_index = pickle.load(f)
            
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
        
        # Convertir defaultdict a dict normal
        self.merged_index = {term: dict(postings) for term, postings in merged_index.items()}
        
        # Guardar índice merged
        final_path = os.path.join(self.output_dir, "final_index.pkl")
        with open(final_path, 'wb') as f:
            pickle.dump(self.merged_index, f)
        
        print(f"✅ Merge completado: {len(self.merged_index)} términos únicos")
        
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
        
        # Usar el índice en memoria si está disponible
        if self.merged_index is None:
            with open(index_path, 'rb') as f:
                self.merged_index = pickle.load(f)
        
        # Calcular TF-IDF para cada término en cada documento
        doc_norms = defaultdict(float)
        
        for term, postings in self.merged_index.items():
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
            'index': self.merged_index,
            'doc_norms': doc_norms,
            'doc_lengths': self.doc_lengths,
            'doc_ids': self.doc_ids,
            'num_documents': self.num_documents,
            'df': dict(self.df)
        }
        
        with open(index_path, 'wb') as f:
            pickle.dump(final_data, f)
        
        print(f"✅ TF-IDF calculado para {len(doc_norms)} documentos")
    
    # ========================================================================
    # MÉTODOS NUEVOS PARA BÚSQUEDA
    # ========================================================================
    
    def finalize_for_search(self):
        """
        Finaliza el índice para búsqueda eficiente.
        Genera archivos auxiliares necesarios para cosine_search.py
        """
        try:
            from spimi_helpers import finalize_index_for_search
        except ImportError:
            print("⚠️  spimi_helpers.py no encontrado")
            print("   Creando versión simplificada...")
            self._finalize_simple()
            return
        
        print("\n🔧 Preparando índice para búsqueda...")
        
        # Verificar que tenemos el índice merged
        if self.merged_index is None:
            merged_path = os.path.join(self.output_dir, 'final_index.pkl')
            if os.path.exists(merged_path):
                print(f"📂 Cargando índice desde {merged_path}")
                with open(merged_path, 'rb') as f:
                    data = pickle.load(f)
                    if isinstance(data, dict) and 'index' in data:
                        self.merged_index = data['index']
                    else:
                        self.merged_index = data
            else:
                raise FileNotFoundError("No se encontró el índice merged")
        
        # Finalizar con el helper
        finalize_index_for_search(
            merged_index=self.merged_index,
            doc_ids=self.doc_ids,
            total_docs=self.num_documents,
            output_dir=self.output_dir,
            block_size=1000
        )
        
        print("✅ Índice listo para búsqueda")
    
    def _finalize_simple(self):
        """Versión simplificada sin spimi_helpers"""
        print("  Guardando metadatos básicos...")
        
        # Cargar índice si es necesario
        final_path = os.path.join(self.output_dir, 'final_index.pkl')
        with open(final_path, 'rb') as f:
            data = pickle.load(f)
        
        # Extraer componentes
        if isinstance(data, dict) and 'index' in data:
            index = data['index']
            doc_norms = data['doc_norms']
            doc_ids = data['doc_ids']
        else:
            print("⚠️  Estructura de índice no reconocida")
            return
        
        # Guardar metadatos
        with open(os.path.join(self.output_dir, 'doc_norms.pkl'), 'wb') as f:
            pickle.dump(doc_norms, f)
        
        with open(os.path.join(self.output_dir, 'doc_ids.pkl'), 'wb') as f:
            pickle.dump(doc_ids, f)
        
        # Calcular IDF
        idf_scores = {}
        for term, postings in index.items():
            df = len(postings)
            idf_scores[term] = math.log10(self.num_documents / df) if df > 0 else 0
        
        with open(os.path.join(self.output_dir, 'idf_scores.pkl'), 'wb') as f:
            pickle.dump(idf_scores, f)
        
        print("  ✓ Metadatos guardados")
    
    def build_complete_index(self, document_generator):
        """
        Construye el índice completo incluyendo preparación para búsqueda.
        
        Args:
            document_generator: Generador de (doc_id, tokens)
        """
        print("="*80)
        print("CONSTRUCCIÓN COMPLETA DEL ÍNDICE SPIMI")
        print("="*80)
        
        # Construir índice normal
        final_path = self.build_index(document_generator)
        
        # Finalizar para búsqueda
        print("\n3️⃣ Finalizando para búsqueda...")
        self.finalize_for_search()
        
        print("\n" + "="*80)
        print("✅ ÍNDICE COMPLETO CONSTRUIDO Y LISTO")
        print("="*80)
        
        return final_path


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


# ============================================================================
# MAIN - EJEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    from preprocessor import TextPreprocessor
    import csv
    
    print("="*80)
    print("CONSTRUCCIÓN DEL ÍNDICE SPIMI CON SOPORTE DE BÚSQUEDA")
    print("="*80)
    
    # Obtener rutas absolutas
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(current_dir)  # Un nivel arriba de text_search
    
    output_dir = os.path.join(project_root, 'data', 'spimi_blocks')
    dataset_path = os.path.join(project_root, 'datasets', 'spotify_songs.csv')
    
    print(f"\n📁 Directorio del proyecto: {project_root}")
    print(f"📁 Directorio de salida: {output_dir}")
    print(f"📁 Dataset: {dataset_path}")
    
    # Inicializar
    indexer = SPIMIIndexer(output_dir=output_dir, block_size_mb=1)
    preprocessor = TextPreprocessor()
    
    # Función generadora de documentos
    def document_generator():
        """
        Generador que lee tu dataset y produce (doc_id, tokens)
        """
        
        if not os.path.exists(dataset_path):
            print(f"⚠️  Dataset no encontrado: {dataset_path}")
            print("   Usando datos de ejemplo...")
            
            # Datos de ejemplo si no existe el dataset
            examples = [
                ("song_1", "amor corazon sentimiento vida"),
                ("song_2", "amor pasion fuego corazon"),
                ("song_3", "ciudad noche luz calle"),
                ("song_4", "amor dolor lagrimas vida"),
                ("song_5", "corazon roto dolor tristeza"),
            ]
            
            for doc_id, text in examples:
                tokens = preprocessor.preprocess(text)
                yield doc_id, tokens
            
            return
        
        # Leer dataset real
        try:
            with open(dataset_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for i, row in enumerate(reader):
                    doc_id = f"song_{i}"
                    
                    # Concatenar campos textuales
                    text = f"{row.get('track_name', '')} {row.get('track_artist', '')} {row.get('lyrics', '')}"
                    
                    # Preprocesar
                    tokens = preprocessor.preprocess(text)
                    
                    yield doc_id, tokens
                    
                    # Mostrar progreso
                    if (i + 1) % 1000 == 0:
                        print(f"  Procesados: {i + 1} documentos")
                    
                    # Limitar para pruebas (quitar este if para procesar todo)
                    if i >= 10000:  # Solo 10k documentos para prueba
                        break
                        
        except Exception as e:
            print(f"❌ Error leyendo dataset: {e}")
            return
    
    # Construir índice completo
    try:
        indexer.build_complete_index(document_generator())
        
        print("\n✅ Construcción exitosa!")
        print(f"📁 Archivos generados en: {indexer.output_dir}")
        
        # Verificar archivos
        required_files = [
            'final_index.pkl',
            'doc_norms.pkl',
            'doc_ids.pkl', 
            'idf_scores.pkl',
        ]
        
        print("\n📋 Verificación de archivos:")
        for filename in required_files:
            path = os.path.join(indexer.output_dir, filename)
            exists = "✓" if os.path.exists(path) else "✗"
            size_mb = os.path.getsize(path) / (1024*1024) if os.path.exists(path) else 0
            print(f"  {exists} {filename:<25} {size_mb:>8.2f} MB")
        
    except Exception as e:
        print(f"\n❌ Error durante la construcción: {str(e)}")
        import traceback
        traceback.print_exc()