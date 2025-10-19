import math
from typing import List, Tuple, Optional, Any
from dataclasses import dataclass
import pickle
import os


@dataclass
class Point:
    """Representa un punto multidimensional con datos asociados"""
    coordinates: List[float]
    data: Any = None  # Datos adicionales (id, registro completo, etc.)
    
    def __getitem__(self, index):
        return self.coordinates[index]
    
    def __len__(self):
        return len(self.coordinates)
    
    def __repr__(self):
        return f"Point({self.coordinates}, data={self.data})"


@dataclass
class MBR:
    """Minimum Bounding Rectangle - Rectángulo mínimo envolvente"""
    lower: List[float]  # Esquina inferior izquierda (L)
    upper: List[float]  # Esquina superior derecha (U)
    
    def __init__(self, lower: List[float], upper: List[float]):
        self.lower = lower
        self.upper = upper
        self.dimensions = len(lower)
    
    def area(self) -> float:
        """Calcula el área (volumen en n-dimensiones) del MBR"""
        area = 1.0
        for i in range(self.dimensions):
            area *= (self.upper[i] - self.lower[i])
        return area
    
    def enlargement(self, other: 'MBR') -> float:
        """Calcula el incremento de área necesario para incluir otro MBR"""
        new_mbr = self.union(other)
        return new_mbr.area() - self.area()
    
    def union(self, other: 'MBR') -> 'MBR':
        """Retorna un MBR que engloba este MBR y otro"""
        new_lower = [min(self.lower[i], other.lower[i]) for i in range(self.dimensions)]
        new_upper = [max(self.upper[i], other.upper[i]) for i in range(self.dimensions)]
        return MBR(new_lower, new_upper)
    
    def intersects(self, other: 'MBR') -> bool:
        """Verifica si este MBR intersecta con otro"""
        for i in range(self.dimensions):
            if self.lower[i] > other.upper[i] or self.upper[i] < other.lower[i]:
                return False
        return True
    
    def contains_point(self, point: Point) -> bool:
        """Verifica si un punto está contenido en el MBR"""
        for i in range(self.dimensions):
            if point[i] < self.lower[i] or point[i] > self.upper[i]:
                return False
        return True
    
    def contains_mbr(self, other: 'MBR') -> bool:
        """Verifica si este MBR contiene completamente a otro MBR"""
        for i in range(self.dimensions):
            if other.lower[i] < self.lower[i] or other.upper[i] > self.upper[i]:
                return False
        return True
    
    @staticmethod
    def from_point(point: Point) -> 'MBR':
        """Crea un MBR desde un único punto"""
        return MBR(point.coordinates.copy(), point.coordinates.copy())
    
    @staticmethod
    def from_points(points: List[Point]) -> 'MBR':
        """Crea un MBR que engloba todos los puntos"""
        if not points:
            raise ValueError("Cannot create MBR from empty point list")
        
        dimensions = len(points[0])
        lower = [min(p[i] for p in points) for i in range(dimensions)]
        upper = [max(p[i] for p in points) for i in range(dimensions)]
        return MBR(lower, upper)
    
    def __repr__(self):
        return f"MBR(lower={self.lower}, upper={self.upper})"


class RTreeNode:
    """Nodo del R-Tree"""
    
    def __init__(self, is_leaf: bool = True):
        self.is_leaf = is_leaf
        self.entries: List[Tuple[MBR, Any]] = []  # (MBR, Point o RTreeNode)
        self.parent: Optional['RTreeNode'] = None
        self.mbr: Optional[MBR] = None
    
    def is_full(self, max_entries: int) -> bool:
        """Verifica si el nodo está lleno"""
        return len(self.entries) >= max_entries
    
    def is_underflow(self, min_entries: int) -> bool:
        """Verifica si el nodo tiene menos entradas del mínimo"""
        return len(self.entries) < min_entries
    
    def update_mbr(self):
        """Actualiza el MBR del nodo para englobar todas sus entradas"""
        if not self.entries:
            self.mbr = None
            return
        
        mbrs = [entry[0] for entry in self.entries]
        lower = [min(mbr.lower[i] for mbr in mbrs) for i in range(mbrs[0].dimensions)]
        upper = [max(mbr.upper[i] for mbr in mbrs) for i in range(mbrs[0].dimensions)]
        self.mbr = MBR(lower, upper)
    
    def __repr__(self):
        return f"RTreeNode(leaf={self.is_leaf}, entries={len(self.entries)}, mbr={self.mbr})"


class RTree:
    """
    Implementación de R-Tree para indexación espacial.
    
    Parámetros:
    - max_entries (M): Máximo número de entradas por nodo
    - min_entries (m): Mínimo número de entradas por nodo (típicamente M/2)
    - dimensions: Número de dimensiones del espacio
    """
    
    def __init__(self, max_entries: int = 4, min_entries: int = 2, dimensions: int = 2):
        self.max_entries = max_entries
        self.min_entries = min_entries
        self.dimensions = dimensions
        self.root = RTreeNode(is_leaf=True)
        self.size = 0  # Número de puntos en el árbol
    
    def add(self, point: Point):
        """
        Inserta un punto en el R-Tree.
        
        Args:
            point: Punto multidimensional a insertar
        """
        if len(point) != self.dimensions:
            raise ValueError(f"Point must have {self.dimensions} dimensions")
        
        mbr = MBR.from_point(point)
        leaf = self._choose_leaf(self.root, mbr)
        leaf.entries.append((mbr, point))
        leaf.update_mbr()
        self.size += 1
        
        # Si el nodo está lleno, dividir
        if leaf.is_full(self.max_entries):
            self._split_node(leaf)
    
    def _choose_leaf(self, node: RTreeNode, mbr: MBR) -> RTreeNode:
        """
        Encuentra el nodo hoja más apropiado para insertar el MBR.
        Usa el criterio de menor incremento de área.
        """
        if node.is_leaf:
            return node
        
        # Encontrar el hijo que requiere menor incremento de área
        min_enlargement = float('inf')
        best_child = None
        
        for entry_mbr, child in node.entries:
            enlargement = entry_mbr.enlargement(mbr)
            if enlargement < min_enlargement:
                min_enlargement = enlargement
                best_child = child
            elif enlargement == min_enlargement:
                # En caso de empate, elegir el de menor área
                if entry_mbr.area() < node.entries[node.entries.index((entry_mbr, best_child))][0].area():
                    best_child = child
        
        return self._choose_leaf(best_child, mbr)
    
    def _split_node(self, node: RTreeNode):
        """
        Divide un nodo usando el algoritmo de split cuadrático.
        """
        # Implementación básica del split cuadrático
        entries = node.entries
        
        # 1. Encontrar los dos elementos más separados
        seed1, seed2 = self._pick_seeds(entries)
        
        # 2. Crear dos nuevos grupos
        group1 = [seed1]
        group2 = [seed2]
        remaining = [e for e in entries if e not in [seed1, seed2]]
        
        # 3. Distribuir los elementos restantes
        while remaining:
            # Asegurar que cada grupo tenga al menos min_entries
            if len(group1) + len(remaining) == self.min_entries:
                group1.extend(remaining)
                break
            if len(group2) + len(remaining) == self.min_entries:
                group2.extend(remaining)
                break
            
            entry = remaining.pop(0)
            # Agregar al grupo que requiere menor incremento
            mbr1 = self._calculate_group_mbr(group1)
            mbr2 = self._calculate_group_mbr(group2)
            
            if mbr1.enlargement(entry[0]) < mbr2.enlargement(entry[0]):
                group1.append(entry)
            else:
                group2.append(entry)
        
        # 4. Actualizar el nodo actual y crear uno nuevo
        node.entries = group1
        node.update_mbr()
        
        new_node = RTreeNode(is_leaf=node.is_leaf)
        new_node.entries = group2
        new_node.update_mbr()
        
        # 5. Ajustar el árbol hacia arriba
        if node.parent is None:
            # Crear nueva raíz
            new_root = RTreeNode(is_leaf=False)
            new_root.entries = [(node.mbr, node), (new_node.mbr, new_node)]
            new_root.update_mbr()
            node.parent = new_root
            new_node.parent = new_root
            self.root = new_root
        else:
            # Insertar nuevo nodo en el padre
            parent = node.parent
            parent.entries.append((new_node.mbr, new_node))
            new_node.parent = parent
            parent.update_mbr()
            
            if parent.is_full(self.max_entries):
                self._split_node(parent)
    
    def _pick_seeds(self, entries: List[Tuple[MBR, Any]]) -> Tuple[Tuple[MBR, Any], Tuple[MBR, Any]]:
        """
        Selecciona las dos entradas más separadas para iniciar el split.
        """
        max_waste = -1
        seed1, seed2 = None, None
        
        for i in range(len(entries)):
            for j in range(i + 1, len(entries)):
                mbr_union = entries[i][0].union(entries[j][0])
                waste = mbr_union.area() - entries[i][0].area() - entries[j][0].area()
                if waste > max_waste:
                    max_waste = waste
                    seed1, seed2 = entries[i], entries[j]
        
        return seed1, seed2
    
    def _calculate_group_mbr(self, group: List[Tuple[MBR, Any]]) -> MBR:
        """Calcula el MBR que engloba un grupo de entradas"""
        mbrs = [entry[0] for entry in group]
        lower = [min(mbr.lower[i] for mbr in mbrs) for i in range(self.dimensions)]
        upper = [max(mbr.upper[i] for mbr in mbrs) for i in range(self.dimensions)]
        return MBR(lower, upper)
    
    def rangeSearch(self, point: Point, radius: float) -> List[Point]:
        """
        Búsqueda por rango: encuentra todos los puntos dentro de un radio
        desde un punto de consulta.
        
        Args:
            point: Punto de consulta
            radius: Radio de búsqueda
            
        Returns:
            Lista de puntos dentro del rango
        """
        if len(point) != self.dimensions:
            raise ValueError(f"Query point must have {self.dimensions} dimensions")
        
        result = []
        self._range_search_recursive(self.root, point, radius, result)
        return result
    
    def _range_search_recursive(self, node: RTreeNode, query_point: Point, 
                                radius: float, result: List[Point]):
        """
        Búsqueda recursiva por rango usando MINDIST como filtro.
        """
        if node.is_leaf:
            # Revisar cada punto en la hoja
            for mbr, point in node.entries:
                distance = self._euclidean_distance(query_point, point)
                if distance <= radius:
                    result.append(point)
        else:
            # Revisar nodos internos usando MINDIST
            for mbr, child in node.entries:
                min_dist = self._mindist(query_point, mbr)
                if min_dist <= radius:
                    self._range_search_recursive(child, query_point, radius, result)
    
    def knnSearch(self, point: Point, k: int) -> List[Tuple[Point, float]]:
        """
        Búsqueda de los K vecinos más cercanos.
        
        Args:
            point: Punto de consulta
            k: Número de vecinos a encontrar
            
        Returns:
            Lista de tuplas (punto, distancia) ordenada por distancia
        """
        if len(point) != self.dimensions:
            raise ValueError(f"Query point must have {self.dimensions} dimensions")
        
        if k <= 0:
            raise ValueError("k must be positive")
        
        # Usar una cola de prioridad para procesar nodos por distancia
        import heapq
        
        # Cola de prioridad: (distancia, es_punto, objeto)
        # es_punto: True si es un punto final, False si es un nodo
        priority_queue = [(0, False, self.root)]
        
        # Lista de k mejores vecinos: (distancia, punto)
        best_neighbors = []
        
        while priority_queue and len(best_neighbors) < k:
            dist, is_point, obj = heapq.heappop(priority_queue)
            
            if is_point:
                # Es un punto final
                heapq.heappush(best_neighbors, (dist, obj))
            else:
                # Es un nodo, explorar sus hijos
                node = obj
                
                if node.is_leaf:
                    # Agregar todos los puntos de la hoja a la cola
                    for mbr, p in node.entries:
                        distance = self._euclidean_distance(point, p)
                        heapq.heappush(priority_queue, (distance, True, p))
                else:
                    # Agregar los hijos a la cola ordenados por MINDIST
                    for mbr, child in node.entries:
                        min_dist = self._mindist(point, mbr)
                        heapq.heappush(priority_queue, (min_dist, False, child))
        
        # Completar con los mejores k vecinos
        result = []
        while best_neighbors and len(result) < k:
            dist, p = heapq.heappop(best_neighbors)
            result.append((p, dist))
        
        return sorted(result, key=lambda x: x[1])
    
    def _mindist(self, point: Point, mbr: MBR) -> float:
        """
        Calcula la distancia mínima entre un punto y un MBR.
        Basado en la fórmula de MINDIST vista en clase.
        
        MINDIST(Q, R) = sqrt(sum((qi - ri)^2))
        donde ri = li si qi < li
                 = ui si qi > ui
                 = qi en otro caso
        """
        sum_sq = 0.0
        for i in range(self.dimensions):
            qi = point[i]
            li = mbr.lower[i]
            ui = mbr.upper[i]
            
            if qi < li:
                ri = li
            elif qi > ui:
                ri = ui
            else:
                ri = qi
            
            sum_sq += (qi - ri) ** 2
        
        return math.sqrt(sum_sq)
    
    def _euclidean_distance(self, p1: Point, p2: Point) -> float:
        """
        Calcula la distancia euclidiana entre dos puntos.
        """
        sum_sq = sum((p1[i] - p2[i]) ** 2 for i in range(self.dimensions))
        return math.sqrt(sum_sq)
    
    def remove(self, point: Point) -> bool:
        """
        Elimina un punto del R-Tree.
        
        Args:
            point: Punto a eliminar
            
        Returns:
            True si se eliminó exitosamente, False si no se encontró
        """
        if len(point) != self.dimensions:
            raise ValueError(f"Point must have {self.dimensions} dimensions")
        
        # Buscar y eliminar el punto
        leaf, index = self._find_leaf(self.root, point)
        
        if leaf is None:
            return False  # Punto no encontrado
        
        # Eliminar la entrada
        del leaf.entries[index]
        leaf.update_mbr()
        self.size -= 1
        
        # Verificar underflow y condensar el árbol
        if leaf.is_underflow(self.min_entries) and leaf != self.root:
            self._condense_tree(leaf)
        
        # Si la raíz tiene solo un hijo, hacer que ese hijo sea la nueva raíz
        if not self.root.is_leaf and len(self.root.entries) == 1:
            self.root = self.root.entries[0][1]
            self.root.parent = None
        
        return True
    
    def _find_leaf(self, node: RTreeNode, point: Point) -> Tuple[Optional[RTreeNode], Optional[int]]:
        """
        Encuentra el nodo hoja que contiene el punto y su índice.
        """
        if node.is_leaf:
            # Buscar el punto en las entradas
            for i, (mbr, p) in enumerate(node.entries):
                if self._points_equal(p, point):
                    return node, i
            return None, None
        else:
            # Buscar en los hijos cuyo MBR contiene el punto
            for mbr, child in node.entries:
                if mbr.contains_point(point):
                    result = self._find_leaf(child, point)
                    if result[0] is not None:
                        return result
            return None, None
    
    def _points_equal(self, p1: Point, p2: Point) -> bool:
        """Verifica si dos puntos son iguales (considerando sus coordenadas)"""
        if len(p1) != len(p2):
            return False
        return all(abs(p1[i] - p2[i]) < 1e-9 for i in range(len(p1)))
    
    def _condense_tree(self, leaf: RTreeNode):
        """
        Condensa el árbol después de una eliminación que causa underflow.
        """
        eliminated_entries = []
        node = leaf
        
        while node != self.root:
            parent = node.parent
            
            if node.is_underflow(self.min_entries):
                # Eliminar el nodo del padre
                parent.entries = [(mbr, child) for mbr, child in parent.entries if child != node]
                parent.update_mbr()
                
                # Guardar las entradas para reinserción
                eliminated_entries.extend(node.entries)
            else:
                # Actualizar MBR del padre
                parent.update_mbr()
            
            node = parent
        
        # Reinsertar las entradas eliminadas
        for mbr, obj in eliminated_entries:
            if isinstance(obj, Point):
                self.add(obj)
    
    def search(self, point: Point) -> List[Point]:
        """
        Búsqueda exacta de un punto.
        
        Args:
            point: Punto a buscar
            
        Returns:
            Lista con el punto si existe, lista vacía si no
        """
        leaf, index = self._find_leaf(self.root, point)
        if leaf is not None:
            return [leaf.entries[index][1]]
        return []
    
    def get_all_points(self) -> List[Point]:
        """
        Retorna todos los puntos almacenados en el R-Tree.
        """
        result = []
        self._collect_points(self.root, result)
        return result
    
    def _collect_points(self, node: RTreeNode, result: List[Point]):
        """Recolecta recursivamente todos los puntos del árbol"""
        if node.is_leaf:
            for mbr, point in node.entries:
                result.append(point)
        else:
            for mbr, child in node.entries:
                self._collect_points(child, result)
    
    def save_to_file(self, filename: str):
        """
        Guarda el R-Tree en un archivo usando pickle.
        
        Args:
            filename: Ruta del archivo donde guardar el índice
        """
        with open(filename, 'wb') as f:
            pickle.dump({
                'max_entries': self.max_entries,
                'min_entries': self.min_entries,
                'dimensions': self.dimensions,
                'root': self.root,
                'size': self.size
            }, f)
    
    @staticmethod
    def load_from_file(filename: str) -> 'RTree':
        """
        Carga un R-Tree desde un archivo.
        
        Args:
            filename: Ruta del archivo del índice
            
        Returns:
            Instancia de RTree cargada
        """
        if not os.path.exists(filename):
            raise FileNotFoundError(f"Index file not found: {filename}")
        
        with open(filename, 'rb') as f:
            data = pickle.load(f)
        
        rtree = RTree(
            max_entries=data['max_entries'],
            min_entries=data['min_entries'],
            dimensions=data['dimensions']
        )
        rtree.root = data['root']
        rtree.size = data['size']
        
        return rtree
    
    def build_from_points(self, points: List[Point]):
        """
        Construye el R-Tree a partir de una lista de puntos.
        Útil para carga masiva inicial.
        
        Args:
            points: Lista de puntos a insertar
        """
        for point in points:
            self.add(point)
    
    def clear(self):
        """Limpia el R-Tree, eliminando todos los puntos"""
        self.root = RTreeNode(is_leaf=True)
        self.size = 0
    
    def get_height(self) -> int:
        """Retorna la altura del árbol"""
        return self._get_height_recursive(self.root)
    
    def _get_height_recursive(self, node: RTreeNode) -> int:
        """Calcula recursivamente la altura del árbol"""
        if node.is_leaf:
            return 1
        else:
            max_height = 0
            for mbr, child in node.entries:
                height = self._get_height_recursive(child)
                max_height = max(max_height, height)
            return max_height + 1
    
    def get_statistics(self) -> dict:
        """
        Retorna estadísticas del R-Tree.
        
        Returns:
            Diccionario con estadísticas del árbol
        """
        stats = {
            'total_points': self.size,
            'height': self.get_height(),
            'max_entries': self.max_entries,
            'min_entries': self.min_entries,
            'dimensions': self.dimensions,
            'total_nodes': 0,
            'leaf_nodes': 0,
            'internal_nodes': 0
        }
        
        self._count_nodes(self.root, stats)
        
        return stats
    
    def _count_nodes(self, node: RTreeNode, stats: dict):
        """Cuenta recursivamente los nodos del árbol"""
        stats['total_nodes'] += 1
        
        if node.is_leaf:
            stats['leaf_nodes'] += 1
        else:
            stats['internal_nodes'] += 1
            for mbr, child in node.entries:
                self._count_nodes(child, stats)
    
    def __len__(self):
        return self.size
    
    def __repr__(self):
        return f"RTree(size={self.size}, max_entries={self.max_entries}, dimensions={self.dimensions})"
