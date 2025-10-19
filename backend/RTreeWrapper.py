"""
RTreeWrapper.py
Interface unificada del R-Tree para integración con el SQL Parser.

Este archivo es el que usarán para integrar el R-Tree
con el parser SQL y el resto del sistema.

"""

from typing import List, Dict, Any, Tuple, Optional
from RTreeSpatialIndex import RTreeSpatialIndex


class RTreeWrapper:
    """
    Wrapper del R-Tree que proporciona una interface simple y uniforme
    para ser utilizada por el parser SQL y otros componentes del sistema.
    
    USO:
    ------------------------
    from RTreeWrapper import RTreeWrapper
    
    # Crear índice
    rtree = RTreeWrapper(table_name="Restaurantes", dimensions=2)
    
    # Cargar desde CSV
    rtree.create_from_csv("restaurantes.csv", ["latitud", "longitud"])
    
    # Búsquedas (desde el parser)
    results = rtree.search_range([10.5, 20.3], 2.0)
    results = rtree.search_knn([10.5, 20.3], 5)
    """
    
    def __init__(self, table_name: str, dimensions: int = 2):
        """
        Inicializa el wrapper del R-Tree.
        
        Args:
            table_name: Nombre de la tabla asociada
            dimensions: Número de dimensiones (2 o 3)
        """
        self.table_name = table_name
        self.dimensions = dimensions
        self.index = RTreeSpatialIndex(
            index_name=f"{table_name}_rtree",
            dimensions=dimensions
        )
    
    # =========================================================================
    # MÉTODOS PARA EL PARSER SQL (Interface simple)
    # =========================================================================
    
    def create_from_csv(self, csv_file: str, coordinate_columns: List[str]) -> int:
        """
        Crea el índice desde un archivo CSV.
        
        Ejemplo SQL:
        CREATE TABLE Restaurantes FROM FILE "restaurantes.csv" 
        USING INDEX rtree("latitud", "longitud")
        
        Args:
            csv_file: Ruta del archivo CSV
            coordinate_columns: Nombres de columnas con coordenadas
            
        Returns:
            Número de registros indexados
        """
        count = self.index.load_from_csv(csv_file, coordinate_columns)
        print(f"✓ Índice R-Tree creado: {count} registros indexados")
        return count
    
    def insert(self, coordinates: List[float], record: Dict[str, Any]) -> int:
        """
        Inserta un nuevo registro.
        
        Ejemplo SQL:
        INSERT INTO Restaurantes VALUES (11, 'Nuevo', ARRAY[-12.05, -77.04])
        
        Args:
            coordinates: Coordenadas del punto
            record: Registro completo (diccionario)
            
        Returns:
            ID asignado al punto
        """
        return self.index.add(coordinates, record)
    
    def delete(self, point_id: int) -> bool:
        """
        Elimina un registro por su ID.
        
        Ejemplo SQL:
        DELETE FROM Restaurantes WHERE id = 5
        
        Args:
            point_id: ID del punto a eliminar
            
        Returns:
            True si se eliminó, False si no existe
        """
        return self.index.remove(point_id)
    
    def search_range(self, center: List[float], radius: float) -> List[Dict]:
        """
        Búsqueda por rango (rangeSearch).
        
        Ejemplo SQL:
        SELECT * FROM Restaurantes 
        WHERE ubicacion IN (POINT(-12.05, -77.04), 2.0)
        
        Args:
            center: Punto central [x, y] o [x, y, z]
            radius: Radio de búsqueda
            
        Returns:
            Lista de registros dentro del rango
        """
        return self.index.rangeSearch(center, radius)
    
    def search_knn(self, center: List[float], k: int) -> List[Dict]:
        """
        Búsqueda K-NN (K vecinos más cercanos).
        
        Ejemplo SQL:
        SELECT * FROM Restaurantes 
        WHERE ubicacion KNN (POINT(-12.05, -77.04), 5)
        
        Args:
            center: Punto de consulta [x, y] o [x, y, z]
            k: Número de vecinos
            
        Returns:
            Lista de k registros más cercanos
        """
        return self.index.kNN(center, k)
    
    def search_exact(self, coordinates: List[float], 
                    tolerance: float = 0.0001) -> List[Dict]:
        """
        Búsqueda exacta por coordenadas (con tolerancia).
        
        Ejemplo SQL:
        SELECT * FROM Restaurantes 
        WHERE ubicacion = POINT(-12.05, -77.04)
        
        Args:
            coordinates: Coordenadas exactas
            tolerance: Tolerancia para considerar igualdad
            
        Returns:
            Lista de registros en esa ubicación
        """
        return self.index.rangeSearch(coordinates, radius=tolerance)
    
    # =========================================================================
    # MÉTODOS AUXILIARES
    # =========================================================================
    
    def get_all_records(self) -> List[Dict]:
        """Retorna todos los registros del índice"""
        return self.index.get_all_points()
    
    def count(self) -> int:
        """Retorna el número total de registros"""
        return len(self.index)
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retorna estadísticas del índice"""
        stats = self.index.get_statistics()
        stats['table_name'] = self.table_name
        return stats
    
    def clear(self):
        """Limpia el índice"""
        self.index.clear()
    
    # =========================================================================
    # MÉTODOS PARA CONVERSIÓN DE COORDENADAS GEOGRÁFICAS
    # =========================================================================
    
    @staticmethod
    def km_to_degrees(km: float) -> float:
        """
        Convierte kilómetros a grados (aproximado).
        Útil para búsquedas geográficas.
        
        1 grado ≈ 111 km
        """
        return km / 111.0
    
    @staticmethod
    def degrees_to_km(degrees: float) -> float:
        """Convierte grados a kilómetros (aproximado)"""
        return degrees * 111.0
    
    def search_range_km(self, center: List[float], radius_km: float) -> List[Dict]:
        """
        Búsqueda por rango usando kilómetros (para coordenadas geográficas).
        
        Args:
            center: Coordenadas [lat, lon]
            radius_km: Radio en kilómetros
            
        Returns:
            Lista de registros con distancia en km
        """
        radius_deg = self.km_to_degrees(radius_km)
        results = self.index.rangeSearch(center, radius_deg)
        
        # Convertir distancias a km
        for r in results:
            r['distance_km'] = self.degrees_to_km(r['distance'])
        
        return results
    
    # =========================================================================
    # REPRESENTACIÓN
    # =========================================================================
    
    def __repr__(self) -> str:
        return (f"RTreeWrapper(table='{self.table_name}', "
                f"dimensions={self.dimensions}, "
                f"records={len(self.index)})")


# =============================================================================
# EJEMPLO DE USO PARA EL PARSER SQL
# =============================================================================

class SQLParserExample:
    """
    Ejemplo de cómo el parser SQL usaría el RTreeWrapper.
    
    Esto es lo que tus compañeros implementarían en su parser.
    """
    
    def __init__(self):
        self.tables = {}  # Diccionario de tablas con sus índices
    
    def execute_create_table(self, table_name: str, csv_file: str, 
                            coord_cols: List[str], dimensions: int = 2):
        """
        Ejecuta: CREATE TABLE Restaurantes FROM FILE "..." 
                 USING INDEX rtree("lat", "lon")
        """
        rtree = RTreeWrapper(table_name, dimensions)
        count = rtree.create_from_csv(csv_file, coord_cols)
        self.tables[table_name] = rtree
        return count
    
    def execute_select_range(self, table_name: str, center: List[float], 
                            radius: float):
        """
        Ejecuta: SELECT * FROM Restaurantes 
                 WHERE ubicacion IN (POINT(...), radius)
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        rtree = self.tables[table_name]
        return rtree.search_range(center, radius)
    
    def execute_select_knn(self, table_name: str, center: List[float], k: int):
        """
        Ejecuta: SELECT * FROM Restaurantes 
                 WHERE ubicacion KNN (POINT(...), k)
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        rtree = self.tables[table_name]
        return rtree.search_knn(center, k)
    
    def execute_insert(self, table_name: str, coordinates: List[float], 
                      record: Dict):
        """
        Ejecuta: INSERT INTO Restaurantes VALUES (...)
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        rtree = self.tables[table_name]
        return rtree.insert(coordinates, record)
    
    def execute_delete(self, table_name: str, point_id: int):
        """
        Ejecuta: DELETE FROM Restaurantes WHERE id = ...
        """
        if table_name not in self.tables:
            raise ValueError(f"Table '{table_name}' not found")
        
        rtree = self.tables[table_name]
        return rtree.delete(point_id)


if __name__ == "__main__":
    print("=" * 80)
    print("EJEMPLO DE USO DEL RTREEWRAPPER PARA EL PARSER")
    print("=" * 80)
    
    # Simular parser SQL
    parser = SQLParserExample()
    
    # 1. CREATE TABLE
    print("\n1. CREATE TABLE FROM CSV")
    print("-" * 80)
    print("SQL: CREATE TABLE Restaurantes FROM FILE 'test_data.csv'")
    print("     USING INDEX rtree('latitud', 'longitud')")
    
    # (Nota: necesitarías crear test_data.csv primero)
    # count = parser.execute_create_table(
    #     'Restaurantes', 
    #     'test_data.csv', 
    #     ['latitud', 'longitud']
    # )
    
    # 2. Ejemplo directo con wrapper
    print("\n2. USO DIRECTO DEL WRAPPER")
    print("-" * 80)
    
    rtree = RTreeWrapper('TestTable', dimensions=2)
    
    # Insertar datos
    print("Insertando puntos...")
    rtree.insert([10.5, 20.3], {"name": "Rest A", "rating": 4.5})
    rtree.insert([10.8, 20.1], {"name": "Rest B", "rating": 4.0})
    rtree.insert([15.2, 25.7], {"name": "Rest C", "rating": 4.8})
    
    print(f"Total: {rtree.count()} registros")
    
    # Búsqueda por rango
    print("\n3. BÚSQUEDA POR RANGO")
    print("SQL: SELECT * FROM TestTable WHERE ubicacion IN (POINT(10.5, 20.3), 1.0)")
    results = rtree.search_range([10.5, 20.3], 1.0)
    print(f"Encontrados: {len(results)} registros")
    for r in results:
        print(f"  - {r['data']['name']}: dist={r['distance']:.4f}")
    
    # Búsqueda KNN
    print("\n4. BÚSQUEDA KNN")
    print("SQL: SELECT * FROM TestTable WHERE ubicacion KNN (POINT(10.5, 20.3), 2)")
    knn = rtree.search_knn([10.5, 20.3], 2)
    print(f"Los 2 más cercanos:")
    for i, r in enumerate(knn, 1):
        print(f"  {i}. {r['data']['name']}: dist={r['distance']:.4f}")
    
    # Estadísticas
    print("\n5. ESTADÍSTICAS")
    stats = rtree.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    # Limpiar
    rtree.clear()
    print("\n✓ Wrapper funcionando correctamente")