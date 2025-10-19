"""
RTreeIndex - Wrapper para integración con el sistema de índices del proyecto
Este módulo proporciona una interfaz unificada para el R-Tree que se integra
con el resto del sistema de base de datos.
"""

from typing import List, Tuple, Any, Optional, Union
from RTree import RTree, Point, MBR
import os
import json


class RTreeIndex:
    """
    Índice espacial basado en R-Tree para el proyecto de Base de Datos 2.
    
    Esta clase proporciona una interfaz consistente con los demás índices del proyecto
    (Sequential File, ISAM, Extendible Hashing, B+ Tree).
    """
    
    def __init__(self, index_file: str, dimensions: int = 2, 
                 max_entries: int = 4, min_entries: int = 2):
        """
        Inicializa el índice R-Tree.
        
        Args:
            index_file: Ruta del archivo donde se guardará el índice
            dimensions: Número de dimensiones del espacio (default: 2)
            max_entries: Máximo número de entradas por nodo (default: 4)
            min_entries: Mínimo número de entradas por nodo (default: 2)
        """
        self.index_file = index_file
        self.dimensions = dimensions
        self.max_entries = max_entries
        self.min_entries = min_entries
        
        # Cargar índice existente o crear uno nuevo
        if os.path.exists(index_file):
            self.rtree = RTree.load_from_file(index_file)
        else:
            self.rtree = RTree(
                max_entries=max_entries,
                min_entries=min_entries,
                dimensions=dimensions
            )
    
    def add(self, coordinates: List[float], record: Any) -> bool:
        """
        Agrega un punto al índice R-Tree.
        
        Args:
            coordinates: Lista de coordenadas del punto
            record: Registro completo asociado al punto
            
        Returns:
            True si se agregó exitosamente
        """
        try:
            if len(coordinates) != self.dimensions:
                raise ValueError(f"Expected {self.dimensions} dimensions, got {len(coordinates)}")
            
            point = Point(coordinates, data=record)
            self.rtree.add(point)
            return True
        except Exception as e:
            print(f"Error adding point: {e}")
            return False
    
    def remove(self, coordinates: List[float]) -> bool:
        """
        Elimina un punto del índice R-Tree.
        
        Args:
            coordinates: Coordenadas del punto a eliminar
            
        Returns:
            True si se eliminó exitosamente, False si no se encontró
        """
        try:
            point = Point(coordinates)
            return self.rtree.remove(point)
        except Exception as e:
            print(f"Error removing point: {e}")
            return False
    
    def search(self, coordinates: List[float]) -> List[Any]:
        """
        Búsqueda exacta de un punto por sus coordenadas.
        
        Args:
            coordinates: Coordenadas del punto a buscar
            
        Returns:
            Lista con los registros encontrados
        """
        try:
            point = Point(coordinates)
            results = self.rtree.search(point)
            return [p.data for p in results]
        except Exception as e:
            print(f"Error searching point: {e}")
            return []
    
    def rangeSearch(self, center: Union[List[float], Tuple[float, ...]], 
                   radius: float) -> List[Tuple[Any, float]]:
        """
        Búsqueda por rango: encuentra todos los puntos dentro de un radio.
        
        Args:
            center: Coordenadas del punto central (puede ser lista o tupla)
            radius: Radio de búsqueda
            
        Returns:
            Lista de tuplas (registro, distancia) ordenada por distancia
        """
        try:
            # Convertir a lista si es tupla
            if isinstance(center, tuple):
                center = list(center)
            
            query_point = Point(center)
            results = self.rtree.rangeSearch(query_point, radius)
            
            # Calcular distancias y retornar con registros
            results_with_distance = []
            for p in results:
                import math
                dist = math.sqrt(sum((center[i] - p[i]) ** 2 for i in range(self.dimensions)))
                results_with_distance.append((p.data, dist))
            
            # Ordenar por distancia
            results_with_distance.sort(key=lambda x: x[1])
            return results_with_distance
        except Exception as e:
            print(f"Error in range search: {e}")
            return []
    
    def knnSearch(self, center: Union[List[float], Tuple[float, ...]], 
                 k: int) -> List[Tuple[Any, float]]:
        """
        Búsqueda de los K vecinos más cercanos.
        
        Args:
            center: Coordenadas del punto de consulta
            k: Número de vecinos a encontrar
            
        Returns:
            Lista de tuplas (registro, distancia) ordenada por distancia
        """
        try:
            # Convertir a lista si es tupla
            if isinstance(center, tuple):
                center = list(center)
            
            query_point = Point(center)
            results = self.rtree.knnSearch(query_point, k)
            
            # Convertir a formato (registro, distancia)
            return [(p.data, dist) for p, dist in results]
        except Exception as e:
            print(f"Error in KNN search: {e}")
            return []
    
    def build_from_file(self, csv_file: str, coordinate_columns: List[str], 
                       delimiter: str = ','):
        """
        Construye el índice R-Tree desde un archivo CSV.
        
        Args:
            csv_file: Ruta del archivo CSV
            coordinate_columns: Nombres de las columnas que contienen coordenadas
            delimiter: Delimitador del CSV (default: ',')
        """
        import csv
        
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f, delimiter=delimiter)
            
            for row in reader:
                try:
                    # Extraer coordenadas
                    coordinates = [float(row[col]) for col in coordinate_columns]
                    
                    # Agregar al índice
                    self.add(coordinates, row)
                except (ValueError, KeyError) as e:
                    print(f"Skipping row due to error: {e}")
                    continue
    
    def save(self):
        """Guarda el índice en disco"""
        try:
            self.rtree.save_to_file(self.index_file)
            return True
        except Exception as e:
            print(f"Error saving index: {e}")
            return False
    
    def load(self):
        """Carga el índice desde disco"""
        try:
            if os.path.exists(self.index_file):
                self.rtree = RTree.load_from_file(self.index_file)
                return True
            return False
        except Exception as e:
            print(f"Error loading index: {e}")
            return False
    
    def clear(self):
        """Limpia el índice, eliminando todos los puntos"""
        self.rtree.clear()
    
    def get_all_records(self) -> List[Any]:
        """Retorna todos los registros almacenados en el índice"""
        points = self.rtree.get_all_points()
        return [p.data for p in points]
    
    def get_statistics(self) -> dict:
        """
        Retorna estadísticas del índice R-Tree.
        
        Returns:
            Diccionario con información del índice
        """
        stats = self.rtree.get_statistics()
        stats['index_file'] = self.index_file
        return stats
    
    def __len__(self) -> int:
        """Retorna el número de puntos en el índice"""
        return len(self.rtree)
    
    def __repr__(self) -> str:
        return f"RTreeIndex(file='{self.index_file}', size={len(self)}, dimensions={self.dimensions})"


# ============================================================================
# EJEMPLO DE USO Y PRUEBAS
# ============================================================================

if __name__ == "__main__":
    import math
    
    print("=" * 70)
    print("PRUEBAS DEL RTREEINDEX - ÍNDICE ESPACIAL")
    print("=" * 70)
    
    # Crear índice
    index_file = "spatial_index.bin"
    rtree_index = RTreeIndex(index_file, dimensions=2, max_entries=4, min_entries=2)
    
    # Datos de ejemplo: Restaurantes con coordenadas (latitud, longitud)
    restaurantes = [
        {"id": 1, "nombre": "Restaurante El Marino", "lat": -12.0464, "lon": -77.0428},
        {"id": 2, "nombre": "La Parrilla", "lat": -12.0489, "lon": -77.0401},
        {"id": 3, "nombre": "Casa UTEC", "lat": -12.0501, "lon": -77.0512},
        {"id": 4, "nombre": "El Rincón Trujillano", "lat": -12.0478, "lon": -77.0445},
        {"id": 5, "nombre": "Sushi Bar", "lat": -12.0455, "lon": -77.0490},
        {"id": 6, "nombre": "Café Central", "lat": -12.0492, "lon": -77.0398},
        {"id": 7, "nombre": "Pizza House", "lat": -12.0510, "lon": -77.0520},
        {"id": 8, "nombre": "Burger King", "lat": -12.0440, "lon": -77.0410},
        {"id": 9, "nombre": "KFC", "lat": -12.0520, "lon": -77.0530},
        {"id": 10, "nombre": "Chifa Chinatown", "lat": -12.0470, "lon": -77.0460},
    ]
    
    # 1. INSERCIÓN
    print("\n1. INSERTANDO RESTAURANTES")
    print("-" * 70)
    for rest in restaurantes:
        coords = [rest["lat"], rest["lon"]]
        rtree_index.add(coords, rest)
        print(f"✓ Insertado: {rest['nombre']:25} | Coords: {coords}")
    
    print(f"\nTotal de restaurantes insertados: {len(rtree_index)}")
    
    # 2. ESTADÍSTICAS
    print("\n2. ESTADÍSTICAS DEL ÍNDICE")
    print("-" * 70)
    stats = rtree_index.get_statistics()
    for key, value in stats.items():
        print(f"  {key:20}: {value}")
    
    # 3. BÚSQUEDA EXACTA
    print("\n3. BÚSQUEDA EXACTA")
    print("-" * 70)
    search_coords = [-12.0464, -77.0428]
    print(f"Buscando restaurante en coordenadas: {search_coords}")
    results = rtree_index.search(search_coords)
    if results:
        for r in results:
            print(f"  ✓ Encontrado: {r['nombre']} (ID: {r['id']})")
    else:
        print("  ✗ No se encontró ningún restaurante")
    
    # 4. BÚSQUEDA POR RANGO
    print("\n4. BÚSQUEDA POR RANGO (RADIO)")
    print("-" * 70)
    center = [-12.0480, -77.0450]  # Centro de búsqueda
    radius_km = 1.0  # 1 km
    # Convertir km a grados aproximadamente (1 grado ≈ 111 km)
    radius_degrees = radius_km / 111.0
    
    print(f"Centro de búsqueda: {center}")
    print(f"Radio: {radius_km} km (~{radius_degrees:.4f} grados)")
    
    range_results = rtree_index.rangeSearch(center, radius_degrees)
    print(f"\nRestaurantes encontrados: {len(range_results)}")
    for i, (record, dist) in enumerate(range_results, 1):
        dist_km = dist * 111.0  # Convertir grados a km aproximadamente
        print(f"  {i}. {record['nombre']:25} | Distancia: {dist_km:.2f} km")
    
    # 5. BÚSQUEDA KNN (K VECINOS MÁS CERCANOS)
    print("\n5. BÚSQUEDA KNN - K VECINOS MÁS CERCANOS")
    print("-" * 70)
    query_point = [-12.0475, -77.0440]  # Punto de consulta (mi ubicación)
    k = 3  # Los 3 más cercanos
    
    print(f"Mi ubicación: {query_point}")
    print(f"Buscando los {k} restaurantes más cercanos...")
    
    knn_results = rtree_index.knnSearch(query_point, k)
    print(f"\nLos {k} restaurantes más cercanos:")
    for i, (record, dist) in enumerate(knn_results, 1):
        dist_km = dist * 111.0  # Convertir a km
        print(f"  {i}. {record['nombre']:25} | Distancia: {dist_km:.2f} km")
    
    # 6. ELIMINACIÓN
    print("\n6. ELIMINACIÓN DE UN REGISTRO")
    print("-" * 70)
    coords_to_remove = [-12.0510, -77.0520]  # Pizza House
    print(f"Eliminando restaurante en coordenadas: {coords_to_remove}")
    
    removed = rtree_index.remove(coords_to_remove)
    if removed:
        print(f"  ✓ Restaurante eliminado exitosamente")
        print(f"  Total restante: {len(rtree_index)} restaurantes")
    else:
        print(f"  ✗ No se encontró el restaurante")
    
    # Verificar eliminación
    print("\nVerificando eliminación...")
    search_removed = rtree_index.search(coords_to_remove)
    if not search_removed:
        print("  ✓ Confirmado: el restaurante ya no existe en el índice")
    
    # 7. GUARDAR ÍNDICE
    print("\n7. PERSISTENCIA - GUARDAR Y CARGAR")
    print("-" * 70)
    print(f"Guardando índice en: {index_file}")
    rtree_index.save()
    print("  ✓ Índice guardado exitosamente")
    
    # Cargar índice
    print(f"\nCargando índice desde: {index_file}")
    rtree_index_loaded = RTreeIndex(index_file, dimensions=2)
    print(f"  ✓ Índice cargado: {len(rtree_index_loaded)} registros")
    
    # Verificar que funciona
    print("\nVerificando funcionalidad del índice cargado...")
    test_knn = rtree_index_loaded.knnSearch([-12.0475, -77.0440], 2)
    print(f"  ✓ KNN test: encontrados {len(test_knn)} vecinos")
    for i, (record, dist) in enumerate(test_knn, 1):
        print(f"    {i}. {record['nombre']}")
    
    # 8. TODOS LOS REGISTROS
    print("\n8. LISTAR TODOS LOS REGISTROS")
    print("-" * 70)
    all_records = rtree_index_loaded.get_all_records()
    print(f"Total de registros en el índice: {len(all_records)}")
    for record in all_records:
        print(f"  • {record['nombre']} (ID: {record['id']})")
    
    # Limpiar archivo temporal
    if os.path.exists(index_file):
        os.remove(index_file)
        print(f"\n✓ Archivo temporal eliminado: {index_file}")
    
    print("\n" + "=" * 70)
    print("TODAS LAS PRUEBAS COMPLETADAS EXITOSAMENTE ✓")
    print("=" * 70)
    
    # 9. EJEMPLO DE USO CON CSV
    print("\n9. EJEMPLO: CONSTRUCCIÓN DESDE CSV")
    print("-" * 70)
    print("""
Para construir el índice desde un archivo CSV:

```python
# Crear índice
rtree_index = RTreeIndex('restaurantes.idx', dimensions=2)

# Construir desde CSV
rtree_index.build_from_file(
    csv_file='restaurantes.csv',
    coordinate_columns=['latitud', 'longitud']
)

# Guardar índice
rtree_index.save()

# Búsqueda por rango
mi_ubicacion = [-12.0475, -77.0440]
radio = 1.0 / 111.0  # 1 km en grados
restaurantes_cercanos = rtree_index.rangeSearch(mi_ubicacion, radio)

for rest, distancia in restaurantes_cercanos:
    print(f"{rest['nombre']} - {distancia * 111:.2f} km")
```
    """)