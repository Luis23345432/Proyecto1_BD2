"""
Script de testing completo para el sistema de búsqueda
Incluye pruebas funcionales y de rendimiento
"""

import time
import sys
import os
from typing import List, Tuple

# Agregar path si es necesario
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from text_search.cosine_search import CosineSearch, search_documents
from text_search.preprocessor import TextPreprocessor


class SearchTester:
    """Clase para probar el sistema de búsqueda"""
    
    def __init__(self, index_dir: str = 'data/spimi_blocks'):
        self.index_dir = index_dir
        self.preprocessor = TextPreprocessor()
        self.searcher = None
        
        # Verificar que el índice existe
        if not self._verify_index():
            raise FileNotFoundError(f"Índice no encontrado en {index_dir}")
        
        # Inicializar searcher
        self.searcher = CosineSearch(index_dir=index_dir)
        print(f"✓ Buscador inicializado con índice de {index_dir}")
    
    def _verify_index(self) -> bool:
        """Verifica que existan los archivos necesarios"""
        required = ['doc_norms.pkl', 'doc_ids.pkl', 'idf_scores.pkl', 'term_to_block.pkl']
        
        for filename in required:
            if not os.path.exists(os.path.join(self.index_dir, filename)):
                print(f"❌ Archivo faltante: {filename}")
                return False
        
        return True
    
    def test_single_query(self, query: str, k: int = 10, show_details: bool = True):
        """
        Prueba una consulta individual
        
        Args:
            query: Texto de la consulta
            k: Número de resultados
            show_details: Si mostrar detalles de los resultados
        
        Returns:
            Tupla (results, time_ms)
        """
        print("\n" + "="*80)
        print(f"🔍 CONSULTA: '{query}'")
        print("="*80)
        
        # Preprocesar consulta
        query_tokens = self.preprocessor.preprocess(query)
        print(f"📝 Tokens: {query_tokens}")
        
        # Ejecutar búsqueda con medición de tiempo
        start_time = time.time()
        results = self.searcher.search(query_tokens, top_k=k)
        elapsed_ms = (time.time() - start_time) * 1000
        
        print(f"\n⏱️  Tiempo de búsqueda: {elapsed_ms:.2f} ms")
        print(f"📊 Resultados encontrados: {len(results)}")
        
        if show_details and results:
            print(f"\n📄 Top-{min(k, len(results))} Documentos:")
            for i, (doc_id, score) in enumerate(results, 1):
                print(f"  {i}. Doc: {doc_id}")
                print(f"     Score: {score:.4f} ({score*100:.2f}%)")
        
        return results, elapsed_ms
    
    def test_multiple_queries(self, queries: List[Tuple[str, int]]):
        """
        Prueba múltiples consultas y recopila estadísticas
        
        Args:
            queries: Lista de tuplas (query, k)
        """
        print("\n" + "="*80)
        print("📊 PRUEBAS MÚLTIPLES")
        print("="*80)
        
        total_time = 0
        all_results = []
        
        for i, (query, k) in enumerate(queries, 1):
            print(f"\n--- Consulta {i}/{len(queries)} ---")
            results, elapsed = self.test_single_query(query, k, show_details=False)
            total_time += elapsed
            all_results.append((query, results, elapsed))
            
            print(f"  → {len(results)} resultados en {elapsed:.2f} ms")
        
        # Estadísticas finales
        print("\n" + "="*80)
        print("📊 ESTADÍSTICAS GENERALES")
        print("="*80)
        print(f"Total de consultas: {len(queries)}")
        print(f"Tiempo total: {total_time:.2f} ms")
        print(f"Tiempo promedio: {total_time/len(queries):.2f} ms")
        print(f"Consulta más rápida: {min(r[2] for r in all_results):.2f} ms")
        print(f"Consulta más lenta: {max(r[2] for r in all_results):.2f} ms")
        
        return all_results
    
    def test_performance_by_k(self, query: str, k_values: List[int] = None):
        """
        Prueba el rendimiento con diferentes valores de K
        
        Args:
            query: Consulta a probar
            k_values: Lista de valores K
        """
        if k_values is None:
            k_values = [5, 10, 20, 50, 100]
        
        print("\n" + "="*80)
        print("⚡ ANÁLISIS DE RENDIMIENTO POR K")
        print("="*80)
        print(f"Consulta: '{query}'")
        
        query_tokens = self.preprocessor.preprocess(query)
        
        results_summary = []
        
        for k in k_values:
            start_time = time.time()
            results = self.searcher.search(query_tokens, top_k=k)
            elapsed_ms = (time.time() - start_time) * 1000
            
            results_summary.append({
                'k': k,
                'time_ms': elapsed_ms,
                'results_count': len(results),
                'time_per_result': elapsed_ms / k if k > 0 else 0
            })
        
        # Mostrar tabla de resultados
        print(f"\n{'K':<10} {'Tiempo (ms)':<15} {'Resultados':<15} {'ms/resultado':<15}")
        print("-" * 60)
        for r in results_summary:
            print(f"{r['k']:<10} {r['time_ms']:<15.2f} {r['results_count']:<15} {r['time_per_result']:<15.3f}")
        
        return results_summary
    
    def test_empty_and_edge_cases(self):
        """Prueba casos borde y consultas vacías"""
        print("\n" + "="*80)
        print("🧪 PRUEBAS DE CASOS BORDE")
        print("="*80)
        
        test_cases = [
            ("", "Consulta vacía"),
            ("xyz123qweasd", "Consulta con términos inexistentes"),
            ("el la de", "Solo stopwords"),
            ("a", "Una sola letra"),
            ("inteligencia artificial machine learning deep learning neural networks", "Consulta muy larga")
        ]
        
        for query, description in test_cases:
            print(f"\n🔬 {description}")
            print(f"   Consulta: '{query}'")
            
            try:
                query_tokens = self.preprocessor.preprocess(query)
                print(f"   Tokens: {query_tokens}")
                
                results = self.searcher.search(query_tokens, top_k=5)
                print(f"   ✓ Resultados: {len(results)}")
                
            except Exception as e:
                print(f"   ❌ Error: {str(e)}")
    
    def compare_with_linear(self, query: str, k: int = 10):
        """
        Simula comparación con búsqueda lineal
        (Para implementación real, necesitas acceso a los docs originales)
        """
        print("\n" + "="*80)
        print("⚖️  COMPARACIÓN: Índice Invertido vs Búsqueda Lineal")
        print("="*80)
        print(f"Consulta: '{query}' | K={k}")
        
        # Búsqueda con índice
        query_tokens = self.preprocessor.preprocess(query)
        start = time.time()
        results_indexed = self.searcher.search(query_tokens, top_k=k)
        time_indexed = (time.time() - start) * 1000
        
        print(f"\n📊 Índice Invertido:")
        print(f"   Tiempo: {time_indexed:.2f} ms")
        print(f"   Resultados: {len(results_indexed)}")
        
        # Aquí podrías implementar búsqueda lineal real si tienes los docs
        print(f"\n📊 Búsqueda Lineal (PostgreSQL):")
        print(f"   Tiempo: [Ejecutar benchmark con PostgreSQL]")
        print(f"   Resultados: [Comparar]")
        
        print(f"\n💡 Para comparación completa:")
        print(f"   1. Carga el mismo dataset en PostgreSQL")
        print(f"   2. Crea índice GIN sobre tsvector")
        print(f"   3. Ejecuta consulta equivalente")
        print(f"   4. Compara tiempos y resultados")
    
    def interactive_mode(self):
        """Modo interactivo de búsqueda"""
        print("\n" + "="*80)
        print("🔎 MODO INTERACTIVO")
        print("="*80)
        print("Comandos:")
        print("  - Escribe tu consulta y presiona Enter")
        print("  - 'exit' o 'quit' para salir")
        print("  - 'stats' para ver estadísticas del índice")
        print("="*80)
        
        while True:
            try:
                query = input("\n🔍 Consulta: ").strip()
                
                if query.lower() in ['exit', 'quit', 'salir']:
                    print("\n👋 ¡Hasta luego!")
                    break
                
                if query.lower() == 'stats':
                    self._show_index_stats()
                    continue
                
                if not query:
                    print("⚠️  Consulta vacía")
                    continue
                
                k = input("📊 Top-K (default 10): ").strip()
                k = int(k) if k.isdigit() else 10
                
                self.test_single_query(query, k)
                
            except KeyboardInterrupt:
                print("\n\n👋 ¡Hasta luego!")
                break
            except Exception as e:
                print(f"❌ Error: {str(e)}")
    
    def _show_index_stats(self):
        """Muestra estadísticas del índice"""
        import pickle
        
        print("\n" + "="*80)
        print("📊 ESTADÍSTICAS DEL ÍNDICE")
        print("="*80)
        
        # Cargar info
        info_path = os.path.join(self.index_dir, 'index_info.pkl')
        if os.path.exists(info_path):
            with open(info_path, 'rb') as f:
                info = pickle.load(f)
            
            print(f"Total de documentos: {info.get('total_documents', 'N/A')}")
            print(f"Total de términos: {info.get('total_terms', 'N/A')}")
            print(f"Longitud promedio de doc: {info.get('avg_doc_length', 0):.2f}")
        else:
            print("⚠️  Información no disponible")
        
        # Listar archivos
        print(f"\nArchivos en {self.index_dir}:")
        for f in sorted(os.listdir(self.index_dir)):
            if f.endswith('.pkl'):
                path = os.path.join(self.index_dir, f)
                size_mb = os.path.getsize(path) / (1024 * 1024)
                print(f"  • {f:<30} {size_mb:>8.2f} MB")


def main():
    """Función principal con menú"""
    
    print("\n" + "="*80)
    print("SISTEMA DE TESTING - BÚSQUEDA CON ÍNDICE INVERTIDO")
    print("Proyecto 2 - Base de Datos II")
    print("="*80)
    
    # Determinar ruta del índice (manejar diferentes ubicaciones)
    possible_paths = [
        'data/spimi_blocks',
        '../data/spimi_blocks',
        'text_search/../data/spimi_blocks'
    ]
    
    index_dir = None
    for path in possible_paths:
        if os.path.exists(path):
            index_dir = path
            break
    
    if index_dir is None:
        print("\n❌ No se encontró el directorio del índice en:")
        for path in possible_paths:
            print(f"   • {path}")
        print("\n💡 Construye el índice primero ejecutando:")
        print("   python text_search/spimi.py")
        return
    
    # Inicializar tester
    try:
        tester = SearchTester(index_dir=index_dir)
    except Exception as e:
        print(f"\n❌ Error al inicializar: {str(e)}")
        print("\n💡 Asegúrate de:")
        print("  1. Haber construido el índice SPIMI")
        print("  2. Haber ejecutado finalize_for_search()")
        print("  3. Verificar que existe data/spimi_blocks/")
        return
    
    # Menú
    while True:
        print("\n" + "="*80)
        print("MENÚ DE PRUEBAS")
        print("="*80)
        print("1. Consulta individual")
        print("2. Múltiples consultas de prueba")
        print("3. Análisis de rendimiento (diferentes K)")
        print("4. Pruebas de casos borde")
        print("5. Modo interactivo")
        print("6. Comparar con búsqueda lineal")
        print("7. Mostrar estadísticas del índice")
        print("8. Salir")
        print("="*80)
        
        choice = input("\nSelecciona una opción: ").strip()
        
        try:
            if choice == '1':
                query = input("\n🔍 Ingresa tu consulta: ").strip()
                k = int(input("📊 Top-K (default 10): ").strip() or "10")
                tester.test_single_query(query, k)
                
            elif choice == '2':
                queries = [
                    ("inteligencia artificial", 10),
                    ("base de datos", 5),
                    ("machine learning", 8),
                    ("python programming", 10),
                ]
                tester.test_multiple_queries(queries)
                
            elif choice == '3':
                query = input("\n🔍 Ingresa tu consulta: ").strip()
                tester.test_performance_by_k(query)
                
            elif choice == '4':
                tester.test_empty_and_edge_cases()
                
            elif choice == '5':
                tester.interactive_mode()
                
            elif choice == '6':
                query = input("\n🔍 Ingresa tu consulta: ").strip()
                k = int(input("📊 Top-K (default 10): ").strip() or "10")
                tester.compare_with_linear(query, k)
                
            elif choice == '7':
                tester._show_index_stats()
                
            elif choice == '8':
                print("\n👋 ¡Hasta luego!")
                break
                
            else:
                print("\n❌ Opción inválida")
                
        except KeyboardInterrupt:
            print("\n\n👋 ¡Hasta luego!")
            break
        except Exception as e:
            print(f"\n❌ Error: {str(e)}")
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    main()