import math
import os
import csv
import json
from typing import List, Tuple, Dict, Any, Optional
from rtree import index


class RTreeSpatialIndex:
    """
    Índice espacial R-Tree con soporte 2D y 3D.
    
    Características:
    - Almacenamiento persistente en disco
    - Búsqueda por rango (rangeSearch)
    - Búsqueda K-vecinos más cercanos (kNN)
    - Inserción y eliminación eficientes
    - Soporte multidimensional (2D, 3D)
    """
    
    def __init__(self, index_name: str = 'rtree_spatial', 
                 dimensions: int = 2,
                 storage_path: str = None):
        """
        Inicializa el índice R-Tree.
        
        Args:
            index_name: Nombre base del índice
            dimensions: Número de dimensiones (2 para 2D, 3 para 3D)
            storage_path: Ruta donde se almacenará el índice
        """
        self.dimensions = dimensions
        self.index_name = index_name
        
        # Configurar ruta de almacenamiento
        if storage_path is None:
            storage_path = os.path.join(
                os.path.dirname(__file__), 
                '..', '..', 'out', 'rtree_index'
            )
        
        os.makedirs(storage_path, exist_ok=True)
        self.index_path = os.path.join(storage_path, index_name)
        
        # Configurar propiedades del R-Tree
        p = index.Property()
        p.dimension = dimensions
        p.storage = index.RT_Disk  # Almacenamiento en disco
        p.overwrite = True
        
        # Crear índice
        self.idx = index.Index(self.index_path, properties=p)
        
        # Almacenar metadatos de los objetos
        self.metadata_path = f"{self.index_path}_metadata.json"
        self.metadata = self._load_metadata()
        
        # Contador para IDs únicos
        self.next_id = max(self.metadata.keys(), default=0) + 1
    
    def _load_metadata(self) -> Dict[int, Dict]:
        """Carga metadatos desde archivo JSON"""
        if os.path.exists(self.metadata_path):
            with open(self.metadata_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {int(k): v for k, v in data.items()}
        return {}
    
    def _save_metadata(self):
        """Guarda metadatos en archivo JSON"""
        with open(self.metadata_path, 'w', encoding='utf-8') as f:
            json.dump(self.metadata, f, ensure_ascii=False, indent=2)
    
    def add(self, coordinates: List[float], data: Dict[str, Any]) -> int:
        """
        Inserta un punto en el índice R-Tree.
        
        Args:
            coordinates: Lista de coordenadas [x, y] o [x, y, z]
            data: Diccionario con datos asociados al punto
            
        Returns:
            ID asignado al punto insertado
            
        Raises:
            ValueError: Si las coordenadas no coinciden con las dimensiones
        """
        if len(coordinates) != self.dimensions:
            raise ValueError(
                f"Se esperaban {self.dimensions} dimensiones, "
                f"pero se recibieron {len(coordinates)}"
            )
        
        # Generar ID único
        point_id = self.next_id
        self.next_id += 1
        
        # Crear bounding box (para un punto, min == max)
        if self.dimensions == 2:
            x, y = coordinates
            bbox = (x, y, x, y)
        else:  # 3D
            x, y, z = coordinates
            bbox = (x, y, z, x, y, z)
        
        # Insertar en el índice
        self.idx.insert(point_id, bbox)
        
        # Guardar metadatos
        self.metadata[point_id] = {
            'coordinates': coordinates,
            'data': data
        }
        self._save_metadata()
        
        return point_id
    
    def remove(self, point_id: int) -> bool:
        """
        Elimina un punto del índice.
        
        Args:
            point_id: ID del punto a eliminar
            
        Returns:
            True si se eliminó exitosamente, False si no existe
        """
        if point_id not in self.metadata:
            return False
        
        # Obtener coordenadas del punto
        coordinates = self.metadata[point_id]['coordinates']
        
        # Crear bounding box
        if self.dimensions == 2:
            x, y = coordinates
            bbox = (x, y, x, y)
        else:  # 3D
            x, y, z = coordinates
            bbox = (x, y, z, x, y, z)
        
        # Eliminar del índice
        self.idx.delete(point_id, bbox)
        
        # Eliminar metadatos
        del self.metadata[point_id]
        self._save_metadata()
        
        return True
    
    def rangeSearch(self, center: List[float], radius: float) -> List[Dict[str, Any]]:
        """
        Búsqueda por rango: encuentra todos los puntos dentro de un radio.
        
        Args:
            center: Coordenadas del centro [x, y] o [x, y, z]
            radius: Radio de búsqueda
            
        Returns:
            Lista de diccionarios con información de los puntos encontrados
        """
        if len(center) != self.dimensions:
            raise ValueError(
                f"Centro debe tener {self.dimensions} dimensiones"
            )
        
        # Crear bounding box de búsqueda
        if self.dimensions == 2:
            x, y = center
            search_bbox = (
                x - radius, y - radius,
                x + radius, y + radius
            )
        else:  # 3D
            x, y, z = center
            search_bbox = (
                x - radius, y - radius, z - radius,
                x + radius, y + radius, z + radius
            )
        
        # Buscar candidatos usando el índice
        candidates = list(self.idx.intersection(search_bbox, objects=True))
        
        results = []
        for candidate in candidates:
            point_id = candidate.id
            
            if point_id not in self.metadata:
                continue
            
            coords = self.metadata[point_id]['coordinates']
            
            # Calcular distancia euclidiana exacta
            dist = self._euclidean_distance(center, coords)
            
            # Verificar si está dentro del radio
            if dist <= radius:
                results.append({
                    'id': point_id,
                    'coordinates': coords,
                    'distance': round(dist, 6),
                    'data': self.metadata[point_id]['data']
                })
        
        # Ordenar por distancia
        results.sort(key=lambda x: x['distance'])
        
        return results
    
    def kNN(self, center: List[float], k: int) -> List[Dict[str, Any]]:
        """
        Búsqueda de K vecinos más cercanos.
        
        Args:
            center: Coordenadas del punto de consulta [x, y] o [x, y, z]
            k: Número de vecinos a buscar
            
        Returns:
            Lista de k puntos más cercanos ordenados por distancia
        """
        if len(center) != self.dimensions:
            raise ValueError(
                f"Centro debe tener {self.dimensions} dimensiones"
            )
        
        # Crear punto de consulta
        if self.dimensions == 2:
            x, y = center
            query_point = (x, y, x, y)
        else:  # 3D
            x, y, z = center
            query_point = (x, y, z, x, y, z)
        
        # Buscar k vecinos más cercanos
        nearest_ids = list(self.idx.nearest(query_point, num_results=k, objects=True))
        
        results = []
        for item in nearest_ids:
            point_id = item.id
            
            if point_id not in self.metadata:
                continue
            
            coords = self.metadata[point_id]['coordinates']
            dist = self._euclidean_distance(center, coords)
            
            results.append({
                'id': point_id,
                'coordinates': coords,
                'distance': round(dist, 6),
                'data': self.metadata[point_id]['data']
            })
        
        # Ordenar por distancia
        results.sort(key=lambda x: x['distance'])
        
        return results[:k]
    
    def _euclidean_distance(self, p1: List[float], p2: List[float]) -> float:
        """Calcula la distancia euclidiana entre dos puntos"""
        return math.sqrt(sum((a - b) ** 2 for a, b in zip(p1, p2)))
    
    def load_from_csv(self, csv_file: str, 
                     coordinate_columns: List[str],
                     id_column: Optional[str] = None) -> int:
        """
        Carga datos desde un archivo CSV.
        
        Args:
            csv_file: Ruta del archivo CSV
            coordinate_columns: Nombres de las columnas con coordenadas
            id_column: Nombre de la columna con ID (opcional)
            
        Returns:
            Número de registros insertados
        """
        if len(coordinate_columns) != self.dimensions:
            raise ValueError(
                f"Se requieren {self.dimensions} columnas de coordenadas"
            )
        
        count = 0
        with open(csv_file, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                try:
                    # Extraer coordenadas
                    coordinates = [float(row[col]) for col in coordinate_columns]
                    
                    # Preparar datos
                    data = dict(row)
                    
                    # Insertar
                    self.add(coordinates, data)
                    count += 1
                    
                except (ValueError, KeyError) as e:
                    print(f"Error en fila: {e}")
                    continue
        
        return count
    
    def get_all_points(self) -> List[Dict[str, Any]]:
        """Retorna todos los puntos almacenados en el índice"""
        results = []
        for point_id, meta in self.metadata.items():
            results.append({
                'id': point_id,
                'coordinates': meta['coordinates'],
                'data': meta['data']
            })
        return results
    
    def get_statistics(self) -> Dict[str, Any]:
        """Retorna estadísticas del índice"""
        return {
            'total_points': len(self.metadata),
            'dimensions': self.dimensions,
            'index_name': self.index_name,
            'storage_path': self.index_path,
            'next_id': self.next_id
        }
    
    def clear(self):
        """Limpia todos los datos del índice"""
        # Cerrar índice actual
        self.idx.close()
        
        # Eliminar archivos
        for ext in ['.dat', '.idx', '_metadata.json']:
            file_path = f"{self.index_path}{ext}"
            if os.path.exists(file_path):
                os.remove(file_path)
        
        # Recrear índice vacío
        p = index.Property()
        p.dimension = self.dimensions
        p.storage = index.RT_Disk
        p.overwrite = True
        
        self.idx = index.Index(self.index_path, properties=p)
        self.metadata = {}
        self.next_id = 1
        self._save_metadata()
    
    def __len__(self) -> int:
        """Retorna el número de puntos en el índice"""
        return len(self.metadata)
    
    def __repr__(self) -> str:
        return (f"RTreeSpatialIndex(name='{self.index_name}', "
                f"dimensions={self.dimensions}, "
                f"points={len(self.metadata)})")
