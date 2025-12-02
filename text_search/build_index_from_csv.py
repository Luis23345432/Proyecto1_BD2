"""
Script para construir el índice SPIMI desde un archivo CSV
Uso: python text_search/build_index_from_csv.py
"""

import os
import sys
import csv
from pathlib import Path

# Agregar path si es necesario
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from text_search.spimi import SPIMIIndexer
from text_search.preprocessor import TextPreprocessor


def list_available_csvs():
    """Lista todos los CSVs disponibles en el proyecto"""
    csv_files = []
    
    # Buscar en directorios comunes
    search_dirs = [
        'postman',
        'datasets',
        'datasets/lyrics',
        '.'
    ]
    
    for dir_path in search_dirs:
        if os.path.exists(dir_path):
            for file in os.listdir(dir_path):
                if file.endswith('.csv'):
                    csv_files.append(os.path.join(dir_path, file))
    
    return csv_files


def preview_csv(csv_path, num_rows=5):
    """Muestra una preview del CSV"""
    print(f"\n📄 Preview de: {csv_path}")
    print("=" * 80)
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
            
            print(f"📋 Columnas: {', '.join(headers)}")
            print(f"\n📊 Primeras {num_rows} filas:")
            print("-" * 80)
            
            for i, row in enumerate(reader):
                if i >= num_rows:
                    break
                print(f"\nFila {i+1}:")
                for key, value in row.items():
                    # Truncar valores largos
                    value_str = str(value)[:100] + "..." if len(str(value)) > 100 else str(value)
                    print(f"  {key}: {value_str}")
            
            # Contar total de filas
            f.seek(0)
            next(f)  # Skip header
            total_rows = sum(1 for _ in f)
            print(f"\n📊 Total de filas en el archivo: {total_rows}")
            
    except Exception as e:
        print(f"❌ Error leyendo CSV: {e}")
        return None
    
    return headers


def select_text_columns(headers):
    """Permite al usuario seleccionar qué columnas usar para indexación"""
    print("\n📝 Selecciona las columnas que contienen TEXTO para indexar:")
    print("   (Puedes seleccionar múltiples columnas, se concatenarán)")
    print("-" * 80)
    
    for i, header in enumerate(headers, 1):
        print(f"  {i}. {header}")
    
    print("\n💡 Ejemplo: Si quieres usar columna 1 y 3, escribe: 1,3")
    selected = input("\nColumnas a usar (separadas por coma): ").strip()
    
    try:
        indices = [int(x.strip()) - 1 for x in selected.split(',')]
        selected_cols = [headers[i] for i in indices if 0 <= i < len(headers)]
        
        if not selected_cols:
            print("⚠️  No se seleccionaron columnas válidas")
            return None
        
        print(f"\n✓ Columnas seleccionadas: {', '.join(selected_cols)}")
        return selected_cols
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None


def build_index_from_csv(csv_path, text_columns, output_dir='data/spimi_blocks', 
                         block_size_mb=100, max_docs=None, language='english'):
    """
    Construye el índice SPIMI desde un CSV
    
    Args:
        csv_path: Ruta al archivo CSV
        text_columns: Lista de columnas a concatenar para indexar
        output_dir: Directorio de salida para el índice
        block_size_mb: Tamaño de bloque en MB
        max_docs: Máximo de documentos a procesar (None = todos)
        language: Idioma para stopwords ('english', 'spanish', etc.)
    """
    print("\n" + "=" * 80)
    print("🚀 INICIANDO CONSTRUCCIÓN DEL ÍNDICE")
    print("=" * 80)
    print(f"📁 Archivo: {csv_path}")
    print(f"📝 Columnas: {', '.join(text_columns)}")
    print(f"💾 Salida: {output_dir}")
    print(f"🌍 Idioma: {language}")
    print(f"📦 Tamaño de bloque: {block_size_mb} MB")
    if max_docs:
        print(f"⚠️  Límite: {max_docs} documentos (para prueba)")
    print("=" * 80)
    
    # Inicializar componentes
    indexer = SPIMIIndexer(output_dir=output_dir, block_size_mb=block_size_mb, language=language)
    preprocessor = TextPreprocessor(language=language, use_stemming=True)
    
    # Diccionario para guardar metadata de documentos
    doc_metadata = {}
    
    # Generador de documentos
    def document_generator():
        """Lee el CSV y genera (doc_id, tokens)"""
        docs_processed = 0
        
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                headers = reader.fieldnames
                
                print("\n📖 Leyendo documentos...")
                
                for i, row in enumerate(reader):
                    # Concatenar columnas seleccionadas
                    text_parts = [str(row.get(col, '')) for col in text_columns]
                    text = ' '.join(text_parts)
                    
                    # Generar ID único
                    doc_id = f"doc_{i+1}"
                    
                    # Guardar metadata del documento (nombre, datos originales)
                    # Intentar encontrar una columna "name" o "title" para mostrar
                    display_name = None
                    for col in ['name', 'title', 'nombre', 'titulo']:
                        if col in row and row[col]:
                            display_name = row[col]
                            break
                    
                    if not display_name:
                        # Si no hay columna de nombre, usar la primera columna textual
                        display_name = row.get(headers[0], doc_id) if headers else doc_id
                    
                    doc_metadata[doc_id] = {
                        'name': display_name,
                        'data': row  # Guardar todos los campos originales
                    }
                    
                    # Preprocesar texto
                    tokens = preprocessor.preprocess(text)
                    
                    # Solo retornar si hay tokens
                    if tokens:
                        yield doc_id, tokens
                        docs_processed += 1
                        
                        # Mostrar progreso
                        if docs_processed % 1000 == 0:
                            print(f"  ✓ {docs_processed} documentos procesados...")
                    
                    # Limitar si se especificó max_docs
                    if max_docs and docs_processed >= max_docs:
                        print(f"\n⚠️  Límite alcanzado: {max_docs} documentos")
                        break
                
                print(f"\n✅ Total procesado: {docs_processed} documentos")
                
        except Exception as e:
            print(f"\n❌ Error leyendo CSV: {e}")
            import traceback
            traceback.print_exc()
    
    # Construir índice completo
    try:
        final_path = indexer.build_complete_index(document_generator())
        
        # Guardar metadata de documentos
        import pickle
        metadata_path = os.path.join(output_dir, 'doc_metadata.pkl')
        with open(metadata_path, 'wb') as f:
            pickle.dump(doc_metadata, f)
        print(f"\n💾 Metadata guardada: {len(doc_metadata)} documentos")
        
        print("\n" + "=" * 80)
        print("✅ ÍNDICE CONSTRUIDO EXITOSAMENTE")
        print("=" * 80)
        print(f"📁 Ubicación: {output_dir}")
        
        # Verificar archivos generados
        print("\n📋 Archivos generados:")
        required_files = [
            'doc_norms.pkl',
            'doc_ids.pkl',
            'idf_scores.pkl',
            'term_to_block.pkl',
            'index_info.pkl'
        ]
        
        total_size = 0
        for filename in required_files:
            path = os.path.join(output_dir, filename)
            if os.path.exists(path):
                size_bytes = os.path.getsize(path)
                total_size += size_bytes
                
                # Mostrar en KB si es muy pequeño
                if size_bytes < 1024 * 1024:  # < 1 MB
                    size_kb = size_bytes / 1024
                    print(f"  ✓ {filename:<25} {size_kb:>8.2f} KB")
                else:
                    size_mb = size_bytes / (1024 * 1024)
                    print(f"  ✓ {filename:<25} {size_mb:>8.2f} MB")
            else:
                print(f"  ✗ {filename:<25} [FALTANTE]")
        
        # Contar bloques
        block_files = [f for f in os.listdir(output_dir) if f.startswith('block_')]
        block_size_bytes = sum(os.path.getsize(os.path.join(output_dir, f)) for f in block_files)
        total_size += block_size_bytes
        
        if block_size_bytes < 1024 * 1024:
            print(f"  ✓ Bloques: {len(block_files)} archivos ({block_size_bytes/1024:.2f} KB)")
        else:
            print(f"  ✓ Bloques: {len(block_files)} archivos ({block_size_bytes/(1024*1024):.2f} MB)")
        
        # Tamaño total
        if total_size < 1024 * 1024:
            print(f"\n📊 Tamaño total del índice: {total_size/1024:.2f} KB")
        else:
            print(f"\n📊 Tamaño total del índice: {total_size/(1024*1024):.2f} MB")
        
        print("\n" + "=" * 80)
        print("🎯 PRÓXIMOS PASOS:")
        print("=" * 80)
        print("1. Ejecuta el sistema de pruebas:")
        print("   python text_search/test_complete_search.py")
        print("\n2. O prueba una búsqueda rápida:")
        print("   from text_search.cosine_search import CosineSearch")
        print("   searcher = CosineSearch(index_dir='data/spimi_blocks')")
        print("   results = searcher.search(['tu', 'consulta'])")
        print("=" * 80)
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante la construcción: {e}")
        import traceback
        traceback.print_exc()
        return False


def main():
    """Flujo principal interactivo"""
    print("\n" + "=" * 80)
    print("🔍 CONSTRUCTOR DE ÍNDICE SPIMI DESDE CSV")
    print("Proyecto 2 - Base de Datos II")
    print("=" * 80)
    
    # Paso 1: Listar CSVs disponibles
    print("\n📂 Buscando archivos CSV...")
    csv_files = list_available_csvs()
    
    if not csv_files:
        print("❌ No se encontraron archivos CSV en el proyecto")
        print("\n💡 Copia tu CSV a una de estas carpetas:")
        print("   • postman/")
        print("   • datasets/")
        print("   • raíz del proyecto")
        return
    
    print(f"\n✓ {len(csv_files)} archivo(s) CSV encontrado(s):")
    for i, csv_file in enumerate(csv_files, 1):
        size_mb = os.path.getsize(csv_file) / (1024 * 1024)
        print(f"  {i}. {csv_file} ({size_mb:.2f} MB)")
    
    # Paso 2: Seleccionar CSV
    print("\n📁 Selecciona un archivo CSV:")
    try:
        choice = int(input("Número: ").strip())
        if choice < 1 or choice > len(csv_files):
            print("❌ Opción inválida")
            return
        
        csv_path = csv_files[choice - 1]
        
    except ValueError:
        print("❌ Entrada inválida")
        return
    
    # Paso 3: Preview y seleccionar columnas
    headers = preview_csv(csv_path)
    if not headers:
        return
    
    text_columns = select_text_columns(headers)
    if not text_columns:
        return
    
    # Paso 4: Configuración adicional
    print("\n⚙️  CONFIGURACIÓN ADICIONAL:")
    
    # Idioma
    print("\n🌍 Idioma para stopwords:")
    print("  1. English (inglés)")
    print("  2. Spanish (español)")
    lang_choice = input("Selecciona (default: 1): ").strip()
    language = 'spanish' if lang_choice == '2' else 'english'
    
    # Límite de documentos
    print("\n📊 ¿Cuántos documentos procesar?")
    print("  • Presiona Enter para procesar TODOS")
    print("  • O escribe un número (ej: 1000 para prueba rápida)")
    max_docs_input = input("Número de documentos: ").strip()
    max_docs = int(max_docs_input) if max_docs_input.isdigit() else None
    
    # Tamaño de bloque
    print("\n💾 Tamaño de bloque en RAM:")
    print("  • 50 MB - Para RAM limitada")
    print("  • 100 MB - Recomendado (default)")
    print("  • 200 MB - Para más velocidad")
    block_input = input("MB (default: 100): ").strip()
    block_size = int(block_input) if block_input.isdigit() else 100
    
    # Confirmación
    print("\n" + "=" * 80)
    print("📋 RESUMEN:")
    print("=" * 80)
    print(f"Archivo: {csv_path}")
    print(f"Columnas: {', '.join(text_columns)}")
    print(f"Idioma: {language}")
    print(f"Documentos: {'TODOS' if not max_docs else max_docs}")
    print(f"Tamaño bloque: {block_size} MB")
    print("=" * 80)
    
    confirm = input("\n¿Proceder con la construcción? (s/n): ").strip().lower()
    if confirm != 's':
        print("❌ Operación cancelada")
        return
    
    # Paso 5: Construir índice
    success = build_index_from_csv(
        csv_path=csv_path,
        text_columns=text_columns,
        output_dir='data/spimi_blocks',
        block_size_mb=block_size,
        max_docs=max_docs,
        language=language
    )
    
    if success:
        print("\n🎉 ¡Índice construido exitosamente!")
    else:
        print("\n❌ Hubo errores durante la construcción")


if __name__ == "__main__":
    main()
