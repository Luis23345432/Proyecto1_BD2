"""
Funciones auxiliares para finalizar el índice SPIMI
Genera los archivos necesarios para búsqueda eficiente
"""

import math
import pickle
import os
from collections import defaultdict
from typing import Dict, List


def compute_idf_scores(merged_index: Dict, total_docs: int) -> Dict[str, float]:
    """
    Calcula los scores IDF para todos los términos.
    IDF = log10(N / df)
    
    Args:
        merged_index: Índice invertido completo {term: {doc_id: data, ...}}
        total_docs: Número total de documentos (N)
        
    Returns:
        Diccionario {término: idf_score}
    """
    print("  📊 Calculando IDF scores...")
    idf_scores = {}
    
    for term, postings in merged_index.items():
        # df = número de documentos que contienen el término
        df = len(postings)
        
        # IDF = log10(N / df)
        idf = math.log10(total_docs / df) if df > 0 else 0
        idf_scores[term] = idf
    
    print(f"     ✓ {len(idf_scores)} términos procesados")
    return idf_scores


def compute_tfidf_weights(merged_index: Dict, idf_scores: Dict) -> Dict:
    """
    Calcula los pesos TF-IDF para cada término en cada documento.
    TF-IDF = (1 + log10(tf)) * IDF
    
    Args:
        merged_index: Índice con estructura {term: {doc_id: {'tf': ..., 'positions': ...}}}
        idf_scores: Scores IDF de los términos
        
    Returns:
        Índice con pesos TF-IDF {term: {doc_id: tfidf}}
    """
    print("  📊 Calculando pesos TF-IDF...")
    weighted_index = {}
    
    for term, postings in merged_index.items():
        weighted_postings = {}
        idf = idf_scores[term]
        
        for doc_id, data in postings.items():
            # Extraer TF del diccionario o usar el valor directamente
            if isinstance(data, dict):
                tf = data.get('tf', 0)
            else:
                tf = data  # Por si acaso es un valor directo
            
            # TF weight: 1 + log10(tf)
            tf_weight = 1 + math.log10(tf) if tf > 0 else 0
            
            # TF-IDF = TF * IDF
            tfidf = tf_weight * idf
            weighted_postings[doc_id] = tfidf
        
        weighted_index[term] = weighted_postings
    
    print(f"     ✓ Pesos TF-IDF calculados")
    return weighted_index


def compute_document_norms(weighted_index: Dict) -> Dict[int, float]:
    """
    Calcula la norma euclidiana de cada documento.
    Norma = sqrt(sum(weight^2 for all terms in doc))
    
    Esta norma se usa para normalizar los vectores de documentos
    en el cálculo de similitud de coseno.
    
    Args:
        weighted_index: Índice con pesos TF-IDF
        
    Returns:
        Diccionario {doc_id: norma}
    """
    print("  📊 Calculando normas de documentos...")
    
    # Acumular la suma de cuadrados por documento
    doc_squared_sums = defaultdict(float)
    
    for term, postings in weighted_index.items():
        for doc_id, weight in postings.items():
            doc_squared_sums[doc_id] += weight ** 2
    
    # Calcular la raíz cuadrada
    doc_norms = {doc_id: math.sqrt(sum_sq) 
                 for doc_id, sum_sq in doc_squared_sums.items()}
    
    print(f"     ✓ {len(doc_norms)} normas calculadas")
    return doc_norms


def normalize_index_by_doc_norms(weighted_index: Dict, doc_norms: Dict) -> Dict:
    """
    Normaliza los pesos TF-IDF dividiendo por la norma del documento.
    
    Esto convierte cada documento en un vector unitario, permitiendo
    calcular la similitud de coseno como un simple producto punto.
    
    cos(q, d) = q · d  (si ambos están normalizados)
    
    Args:
        weighted_index: Índice con pesos TF-IDF
        doc_norms: Normas de los documentos
        
    Returns:
        Índice normalizado {term: {doc_id: normalized_weight}}
    """
    print("  📊 Normalizando índice...")
    normalized_index = {}
    
    for term, postings in weighted_index.items():
        normalized_postings = {}
        
        for doc_id, weight in postings.items():
            norm = doc_norms.get(doc_id, 1.0)
            if norm > 0:
                normalized_postings[doc_id] = weight / norm
            else:
                normalized_postings[doc_id] = 0
        
        normalized_index[term] = normalized_postings
    
    print(f"     ✓ Índice normalizado")
    return normalized_index


def save_blocks_to_disk(normalized_index: Dict, output_dir: str, 
                        block_size: int = 1000) -> List[str]:
    """
    Guarda el índice normalizado en bloques en disco.
    
    Args:
        normalized_index: Índice normalizado
        output_dir: Directorio de salida
        block_size: Número de términos por bloque
        
    Returns:
        Lista de nombres de archivos de bloques creados
    """
    print("  💾 Guardando bloques en disco...")
    
    terms = sorted(normalized_index.keys())
    block_files = []
    
    for i in range(0, len(terms), block_size):
        block_terms = terms[i:i + block_size]
        block = {term: normalized_index[term] for term in block_terms}
        
        # Guardar bloque
        block_file = f'block_{len(block_files)}.pkl'
        block_path = os.path.join(output_dir, block_file)
        
        with open(block_path, 'wb') as f:
            pickle.dump(block, f)
        
        block_files.append(block_file)
    
    print(f"     ✓ {len(block_files)} bloques guardados")
    return block_files


def create_term_to_block_mapping(block_files: List[str], output_dir: str):
    """
    Crea un mapeo de términos a archivos de bloque.
    
    Este mapeo permite encontrar rápidamente en qué bloque está
    cada término sin tener que cargar todos los bloques.
    
    Args:
        block_files: Lista de archivos de bloques
        output_dir: Directorio donde están los bloques
    """
    print("  🗺️  Creando mapeo de términos a bloques...")
    term_mapping = {}
    
    for block_file in block_files:
        block_path = os.path.join(output_dir, block_file)
        
        with open(block_path, 'rb') as f:
            block_index = pickle.load(f)
        
        # Mapear cada término a su bloque
        for term in block_index.keys():
            term_mapping[term] = block_file
    
    # Guardar el mapeo
    mapping_path = os.path.join(output_dir, 'term_to_block.pkl')
    with open(mapping_path, 'wb') as f:
        pickle.dump(term_mapping, f)
    
    print(f"     ✓ Mapeo guardado: {len(term_mapping)} términos")


def save_metadata(output_dir: str, doc_norms: Dict, doc_ids: Dict, 
                 idf_scores: Dict, total_docs: int):
    """
    Guarda los metadatos necesarios para la búsqueda.
    
    Args:
        output_dir: Directorio de salida
        doc_norms: Normas de documentos
        doc_ids: Mapeo de doc_id numérico a identificador original
        idf_scores: Scores IDF de términos
        total_docs: Número total de documentos
    """
    print("  💾 Guardando metadatos...")
    
    # Guardar normas de documentos
    norms_path = os.path.join(output_dir, 'doc_norms.pkl')
    with open(norms_path, 'wb') as f:
        pickle.dump(doc_norms, f)
    print(f"     ✓ Normas guardadas: {len(doc_norms)} documentos")
    
    # Guardar IDs de documentos
    ids_path = os.path.join(output_dir, 'doc_ids.pkl')
    with open(ids_path, 'wb') as f:
        pickle.dump(doc_ids, f)
    print(f"     ✓ IDs guardados")
    
    # Guardar scores IDF
    idf_path = os.path.join(output_dir, 'idf_scores.pkl')
    with open(idf_path, 'wb') as f:
        pickle.dump(idf_scores, f)
    print(f"     ✓ IDF scores guardados: {len(idf_scores)} términos")
    
    # Guardar información general del índice
    info = {
        'total_documents': total_docs,
        'total_terms': len(idf_scores),
        'avg_doc_norm': sum(doc_norms.values()) / len(doc_norms) if doc_norms else 0
    }
    info_path = os.path.join(output_dir, 'index_info.pkl')
    with open(info_path, 'wb') as f:
        pickle.dump(info, f)
    print(f"     ✓ Información general guardada")


def finalize_index_for_search(merged_index: Dict, doc_ids: Dict, 
                               total_docs: int, output_dir: str,
                               block_size: int = 1000):
    """
    Procesa el índice merged y lo prepara para búsqueda eficiente.
    
    Esta función debe llamarse DESPUÉS del merge de bloques en SPIMI.
    
    Proceso:
    1. Calcula IDF para todos los términos
    2. Calcula pesos TF-IDF
    3. Calcula normas de documentos
    4. Normaliza el índice
    5. Guarda en bloques en disco
    6. Crea mapeo de términos
    7. Guarda metadatos
    
    Args:
        merged_index: Índice después del merge {term: {doc_id: {'tf': ..., 'positions': ...}}}
        doc_ids: Mapeo de doc_id numérico a identificador original
        total_docs: Número total de documentos
        output_dir: Directorio de salida
        block_size: Términos por bloque (default: 1000)
    """
    print("\n" + "="*80)
    print("🔧 FINALIZANDO ÍNDICE PARA BÚSQUEDA")
    print("="*80)
    
    # 1. Calcular IDF scores
    idf_scores = compute_idf_scores(merged_index, total_docs)
    
    # 2. Calcular pesos TF-IDF
    weighted_index = compute_tfidf_weights(merged_index, idf_scores)
    
    # 3. Calcular normas de documentos
    doc_norms = compute_document_norms(weighted_index)
    
    # 4. Normalizar el índice
    normalized_index = normalize_index_by_doc_norms(weighted_index, doc_norms)
    
    # 5. Guardar índice en bloques
    block_files = save_blocks_to_disk(normalized_index, output_dir, block_size)
    
    # 6. Crear mapeo de términos a bloques
    create_term_to_block_mapping(block_files, output_dir)
    
    # 7. Guardar metadatos
    save_metadata(output_dir, doc_norms, doc_ids, idf_scores, total_docs)
    
    print("\n" + "="*80)
    print("✅ ÍNDICE FINALIZADO Y LISTO PARA BÚSQUEDA")
    print("="*80)
    print(f"📊 Estadísticas:")
    print(f"   • Total de documentos: {total_docs}")
    print(f"   • Total de términos únicos: {len(normalized_index)}")
    print(f"   • Bloques generados: {len(block_files)}")
    print(f"   • Directorio: {output_dir}")
    print("="*80)


if __name__ == "__main__":
    print("Módulo de helpers SPIMI cargado correctamente")
    print("\nUso:")
    print("  from spimi_helpers import finalize_index_for_search")
    print("  finalize_index_for_search(merged_index, doc_ids, total_docs, output_dir)")