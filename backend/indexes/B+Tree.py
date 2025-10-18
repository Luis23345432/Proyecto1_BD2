"""
B+ Tree Index Implementation
Implementación modular lista para integración con sistema de base de datos
Autor: Luis Lopez
"""

import math
from typing import Any, List, Optional, Tuple
from abc import ABC, abstractmethod


# INTERFACES ABSTRACTAS

class IndexInterface(ABC):
    """Interfaz base para todos los índices del proyecto"""

    @abstractmethod
    def search(self, key: Any) -> List[Any]:
        """Busca registros por clave exacta"""
        pass

    @abstractmethod
    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        """Busca registros en un rango de claves"""
        pass

    @abstractmethod
    def add(self, key: Any, record: Any) -> bool:
        """Inserta un nuevo registro"""
        pass

    @abstractmethod
    def remove(self, key: Any) -> bool:
        """Elimina un registro por clave"""
        pass

    @abstractmethod
    def get_stats(self) -> dict:
        """Retorna estadísticas de operaciones (para métricas)"""
        pass


#ESTRUCTURAS DE DATOS

class Record:
    """Registro genérico que almacena datos"""

    def __init__(self, key: Any, data: Any, page_id: int = None, slot_id: int = None):
        self.key = key
        self.data = data  # Puede ser dict, tuple, objeto, etc.
        self.page_id = page_id
        self.slot_id = slot_id

    def __repr__(self):
        return f"Record(key={self.key}, page={self.page_id}, slot={self.slot_id})"

    def to_dict(self):
        """Serialización para persistencia futura"""
        return {
            'key': self.key,
            'data': self.data,
            'page_id': self.page_id,
            'slot_id': self.slot_id
        }


class DataPage:
    """Página física del archivo de datos"""

    def __init__(self, page_id: int, capacity: int = 4):
        self.page_id = page_id
        self.capacity = capacity
        self.records: List[Record] = []
        self.overflow_page: Optional['DataPage'] = None

    def has_space(self) -> bool:
        return len(self.records) < self.capacity

    def insert_record(self, record: Record) -> bool:
        """Inserta un registro en la página"""
        if self.has_space():
            record.page_id = self.page_id
            record.slot_id = len(self.records)
            self.records.append(record)
            return True
        return False

    def get_record(self, slot_id: int) -> Optional[Record]:
        """Obtiene un registro por su slot"""
        if 0 <= slot_id < len(self.records):
            return self.records[slot_id]
        return None

    def remove_record(self, slot_id: int) -> bool:
        """Elimina un registro de la página"""
        if 0 <= slot_id < len(self.records):
            self.records.pop(slot_id)
            # Reajustar slot_ids de los registros restantes
            for i, rec in enumerate(self.records):
                rec.slot_id = i
            return True
        return False

    def __repr__(self):
        return f"Page[{self.page_id}]: {len(self.records)}/{self.capacity} records"


class DataFile:
    """Gestor de archivo de datos (con/sin orden)"""

    def __init__(self, filename: str = "data.bin", page_capacity: int = 4, is_clustered: bool = False):
        self.filename = filename
        self.pages: List[DataPage] = [DataPage(0, page_capacity)]
        self.is_clustered = is_clustered
        self.page_capacity = page_capacity
        self.next_page_id = 1

        # Contadores para métricas
        self.read_count = 0
        self.write_count = 0

    def insert_record(self, record: Record) -> Record:
        """Inserta un registro en el archivo"""
        self.write_count += 1

        if self.is_clustered:
            page = self._find_ordered_page(record.key)
        else:
            page = self._find_page_with_space()

        if page.insert_record(record):
            return record
        else:
            # Crear página de desbordamiento
            overflow = DataPage(self.next_page_id, self.page_capacity)
            self.next_page_id += 1
            page.overflow_page = overflow
            self.pages.append(overflow)
            overflow.insert_record(record)
            return record

    def get_record(self, page_id: int, slot_id: int) -> Optional[Record]:
        """Obtiene un registro por su dirección física"""
        self.read_count += 1

        for page in self.pages:
            if page.page_id == page_id:
                return page.get_record(slot_id)
        return None

    def remove_record(self, page_id: int, slot_id: int) -> bool:
        """Elimina un registro del archivo"""
        self.write_count += 1

        for page in self.pages:
            if page.page_id == page_id:
                return page.remove_record(slot_id)
        return False

    def _find_ordered_page(self, key: Any) -> DataPage:
        """Para archivos clustered: encuentra página ordenada"""
        for i, page in enumerate(self.pages):
            if page.overflow_page:
                continue

            if not page.records:
                return page

            if key <= page.records[-1].key:
                if page.has_space():
                    return page
                return page

            if i < len(self.pages) - 1:
                next_page = self.pages[i + 1]
                if next_page.overflow_page:
                    continue
                if key < next_page.records[0].key if next_page.records else True:
                    if page.has_space():
                        return page
                    return page

        last_page = self.pages[-1]
        if last_page.has_space():
            return last_page

        # Expansión del archivo
        new_page = DataPage(self.next_page_id, self.page_capacity)
        self.next_page_id += 1
        self.pages.append(new_page)
        return new_page

    def _find_page_with_space(self) -> DataPage:
        """Para archivos unclustered: encuentra página con espacio"""
        for page in self.pages:
            if page.has_space():
                return page

        new_page = DataPage(self.next_page_id, self.page_capacity)
        self.next_page_id += 1
        self.pages.append(new_page)
        return new_page

    def get_stats(self) -> dict:
        """Retorna estadísticas de I/O"""
        return {
            'reads': self.read_count,
            'writes': self.write_count,
            'total_pages': len(self.pages),
            'records': sum(len(p.records) for p in self.pages)
        }


#ÍNDICE B+ TREE

class IndexEntry:
    """Entrada en nodo hoja del índice"""

    def __init__(self, key: Any, page_id: int, slot_id: int):
        self.key = key
        self.page_id = page_id
        self.slot_id = slot_id
        self.bucket: List[Tuple[int, int]] = []  # Para duplicados (page_id, slot_id)

    def __repr__(self):
        return f"({self.key} → P{self.page_id}:S{self.slot_id})"


class BPlusNode:
    """Nodo del árbol B+"""

    def __init__(self, degree: int, is_leaf: bool = False):
        self.degree = degree
        self.is_leaf = is_leaf
        self.keys: List[Any] = []
        self.children: List[Any] = []  # IndexEntry o BPlusNode
        self.next: Optional['BPlusNode'] = None
        self.parent: Optional['BPlusNode'] = None

    def is_full(self) -> bool:
        return len(self.keys) >= self.degree - 1

    def is_underflow(self) -> bool:
        """Verifica si tiene menos de la mitad de entradas"""
        if self.degree == 3:
            min_keys = 1
        else:
            min_keys = math.ceil(self.degree / 2) - 1
        return len(self.keys) < min_keys

    def __repr__(self):
        return f"{'LEAF' if self.is_leaf else 'INTERNAL'}({self.keys})"


class BPlusTree(IndexInterface):
    """
    Implementación completa del índice B+ Tree
    Soporta operaciones: search, range_search, add, remove
    Puede ser clustered o unclustered
    """

    def __init__(self,
                 degree: int = 3,
                 data_file: Optional[DataFile] = None,
                 is_clustered: bool = False,
                 verbose: bool = False):
        """
        Args:
            degree: Grado del árbol (mínimo 3)
            data_file: Archivo de datos asociado (si es None, se crea uno nuevo)
            is_clustered: Si el índice está agrupado
            verbose: Si se imprimen mensajes de debug
        """
        if degree < 3:
            raise ValueError("El grado debe ser al menos 3")

        self.degree = degree
        self.root = BPlusNode(degree, is_leaf=True)
        self.is_clustered = is_clustered
        self.verbose = verbose

        # Archivo de datos (se puede compartir entre índices)
        if data_file is None:
            self.data_file = DataFile(is_clustered=is_clustered)
        else:
            self.data_file = data_file

        # Contadores para métricas
        self.search_count = 0
        self.insert_count = 0
        self.delete_count = 0
        self.split_count = 0
        self.merge_count = 0

        if self.verbose:
            print(f"B+ Tree {'CLUSTERED' if is_clustered else 'UNCLUSTERED'} creado (grado={degree})")

    #OPERACIONES PRINCIPALES

    def search(self, key: Any) -> List[Record]:
        """Busca todos los registros con la clave especificada"""
        self.search_count += 1

        if self.verbose:
            print(f"\n>>> SEARCH key={key}")

        entry = self._find_entry(self.root, key)
        if not entry:
            if self.verbose:
                print(f"  Clave {key} no encontrada")
            return []

        # Obtener registro(s) del archivo de datos
        results = [self.data_file.get_record(entry.page_id, entry.slot_id)]

        # Si hay bucket (duplicados)
        for page_id, slot_id in entry.bucket:
            record = self.data_file.get_record(page_id, slot_id)
            if record:
                results.append(record)

        # Filtrar None values
        results = [r for r in results if r is not None]

        if self.verbose:
            print(f"  Encontrados {len(results)} registro(s)")

        return results

    def range_search(self, begin_key: Any, end_key: Any) -> List[Record]:
        """Busca todos los registros en el rango [begin_key, end_key]"""
        self.search_count += 1

        if self.verbose:
            print(f"\n>>> RANGE SEARCH [{begin_key}, {end_key}]")

        leaf = self._find_leaf(self.root, begin_key)
        results = []

        while leaf:
            for i, key in enumerate(leaf.keys):
                if begin_key <= key <= end_key:
                    entry = leaf.children[i]
                    record = self.data_file.get_record(entry.page_id, entry.slot_id)
                    if record:
                        results.append(record)

                    # Agregar bucket si existe
                    for page_id, slot_id in entry.bucket:
                        record = self.data_file.get_record(page_id, slot_id)
                        if record:
                            results.append(record)
                elif key > end_key:
                    if self.verbose:
                        print(f"  Encontrados {len(results)} registros")
                    return results
            leaf = leaf.next

        if self.verbose:
            print(f"  Encontrados {len(results)} registros")

        return results

    def add(self, key: Any, record_data: Any) -> bool:
        """
        Inserta un nuevo registro
        Args:
            key: Clave del registro
            record_data: Datos del registro (dict, tuple, objeto, etc.)
        Returns:
            True si se insertó correctamente
        """
        self.insert_count += 1

        if self.verbose:
            print(f"\n>>> INSERT key={key}")

        # 1. Verificar si la clave ya existe (para unclustered con duplicados)
        existing_entry = self._find_entry(self.root, key)

        if existing_entry and not self.is_clustered:
            # Clave duplicada en índice unclustered
            if self.verbose:
                print(f"  Clave duplicada {key}, agregando al bucket")
            record = self.data_file.insert_record(Record(key, record_data))
            existing_entry.bucket.append((record.page_id, record.slot_id))
            return True

        # 2. Insertar el registro en el archivo de datos
        record = self.data_file.insert_record(Record(key, record_data))

        # 3. Insertar en el índice
        if self.root.is_full():
            old_root = self.root
            self.root = BPlusNode(self.degree, is_leaf=False)
            self.root.children.append(old_root)
            old_root.parent = self.root
            self._split_child(self.root, 0)

        self._insert_non_full(self.root, IndexEntry(key, record.page_id, record.slot_id))
        return True

    def remove(self, key: Any) -> bool:
        """
        Elimina un registro por clave
        Returns:
            True si se eliminó correctamente
        """
        self.delete_count += 1

        if self.verbose:
            print(f"\n>>> DELETE key={key}")

        deleted = self._delete(self.root, key)

        # Si la raíz quedó vacía, bajar nivel
        if not self.root.is_leaf and len(self.root.keys) == 0:
            if len(self.root.children) > 0:
                self.root = self.root.children[0]
                self.root.parent = None
                if self.verbose:
                    print("  Raíz vacía, altura reducida")

        return deleted

    def get_stats(self) -> dict:
        """Retorna estadísticas de operaciones"""
        return {
            'index_type': 'B+Tree',
            'clustered': self.is_clustered,
            'degree': self.degree,
            'searches': self.search_count,
            'inserts': self.insert_count,
            'deletes': self.delete_count,
            'splits': self.split_count,
            'merges': self.merge_count,
            'tree_height': self._get_height(self.root),
            'data_file_stats': self.data_file.get_stats()
        }

    #MÉTODOS AUXILIARES

    def _find_entry(self, node: BPlusNode, key: Any) -> Optional[IndexEntry]:
        """Encuentra una entrada en el árbol"""
        if node.is_leaf:
            for entry in node.children:
                if entry.key == key:
                    return entry
            return None
        else:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            return self._find_entry(node.children[i], key)

    def _find_leaf(self, node: BPlusNode, key: Any) -> BPlusNode:
        """Encuentra el nodo hoja para una clave"""
        if node.is_leaf:
            return node
        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        return self._find_leaf(node.children[i], key)

    def _insert_non_full(self, node: BPlusNode, entry: IndexEntry):
        """Inserta en un nodo que no está lleno"""
        if node.is_leaf:
            i = 0
            while i < len(node.keys) and node.keys[i] < entry.key:
                i += 1
            node.keys.insert(i, entry.key)
            node.children.insert(i, entry)
            if self.verbose:
                print(f"  Insertado en hoja: {entry}")
        else:
            i = 0
            while i < len(node.keys) and entry.key >= node.keys[i]:
                i += 1

            if node.children[i].is_full():
                self._split_child(node, i)
                if entry.key >= node.keys[i]:
                    i += 1

            self._insert_non_full(node.children[i], entry)

    def _split_child(self, parent: BPlusNode, index: int):
        """Divide un nodo hijo lleno"""
        self.split_count += 1

        full_node = parent.children[index]
        mid = len(full_node.keys) // 2

        new_node = BPlusNode(self.degree, is_leaf=full_node.is_leaf)
        new_node.parent = parent

        if full_node.is_leaf:
            new_node.keys = full_node.keys[mid:]
            new_node.children = full_node.children[mid:]
            full_node.keys = full_node.keys[:mid]
            full_node.children = full_node.children[:mid]

            new_node.next = full_node.next
            full_node.next = new_node

            promoted_key = new_node.keys[0]
        else:
            new_node.keys = full_node.keys[mid + 1:]
            new_node.children = full_node.children[mid + 1:]
            promoted_key = full_node.keys[mid]
            full_node.keys = full_node.keys[:mid]
            full_node.children = full_node.children[:mid + 1]

            for child in new_node.children:
                child.parent = new_node

        parent.keys.insert(index, promoted_key)
        parent.children.insert(index + 1, new_node)

        if self.verbose:
            print(f"  Split: promoción de clave {promoted_key}")

    def _delete(self, node: BPlusNode, key: Any) -> bool:
        """Elimina recursivamente con rebalanceo"""
        if node.is_leaf:
            if key in node.keys:
                idx = node.keys.index(key)
                entry = node.children[idx]

                # Eliminar del archivo de datos
                self.data_file.remove_record(entry.page_id, entry.slot_id)

                # Eliminar del índice
                node.keys.pop(idx)
                node.children.pop(idx)

                if self.verbose:
                    print(f"  Clave {key} eliminada de hoja")
                return True
            else:
                if self.verbose:
                    print(f"  Clave {key} no encontrada")
                return False
        else:
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1

            child = node.children[i]
            deleted = self._delete(child, key)

            if deleted and child.is_underflow():
                self._rebalance(node, i)

            return deleted

    def _rebalance(self, parent: BPlusNode, child_idx: int):
        """Rebalancea un nodo con pocas entradas"""
        child = parent.children[child_idx]

        if self.verbose:
            print(f"  Rebalanceando nodo con {len(child.keys)} claves")

        if child_idx > 0:
            left_sibling = parent.children[child_idx - 1]
            if len(left_sibling.keys) > math.ceil(self.degree / 2) - 1:
                self._redistribute_left(parent, child_idx)
                return

        if child_idx < len(parent.children) - 1:
            right_sibling = parent.children[child_idx + 1]
            if len(right_sibling.keys) > math.ceil(self.degree / 2) - 1:
                self._redistribute_right(parent, child_idx)
                return

        if child_idx > 0:
            self._merge(parent, child_idx - 1)
        else:
            self._merge(parent, child_idx)

    def _redistribute_left(self, parent: BPlusNode, child_idx: int):
        """Redistribuir desde hermano izquierdo"""
        child = parent.children[child_idx]
        left = parent.children[child_idx - 1]

        if child.is_leaf:
            child.keys.insert(0, left.keys[-1])
            child.children.insert(0, left.children[-1])
            left.keys.pop()
            left.children.pop()
            parent.keys[child_idx - 1] = child.keys[0]
        else:
            child.keys.insert(0, parent.keys[child_idx - 1])
            parent.keys[child_idx - 1] = left.keys.pop()
            child.children.insert(0, left.children.pop())
            child.children[0].parent = child

        if self.verbose:
            print("  Redistribución desde hermano izquierdo")

    def _redistribute_right(self, parent: BPlusNode, child_idx: int):
        """Redistribuir desde hermano derecho"""
        child = parent.children[child_idx]
        right = parent.children[child_idx + 1]

        if child.is_leaf:
            child.keys.append(right.keys[0])
            child.children.append(right.children[0])
            right.keys.pop(0)
            right.children.pop(0)
            parent.keys[child_idx] = right.keys[0]
        else:
            child.keys.append(parent.keys[child_idx])
            parent.keys[child_idx] = right.keys.pop(0)
            child.children.append(right.children.pop(0))
            child.children[-1].parent = child

        if self.verbose:
            print("  Redistribución desde hermano derecho")

    def _merge(self, parent: BPlusNode, left_idx: int):
        """Fusiona dos nodos hermanos"""
        self.merge_count += 1

        left = parent.children[left_idx]
        right = parent.children[left_idx + 1]

        if left.is_leaf:
            left.keys.extend(right.keys)
            left.children.extend(right.children)
            left.next = right.next
        else:
            left.keys.append(parent.keys[left_idx])
            left.keys.extend(right.keys)
            left.children.extend(right.children)
            for child in right.children:
                child.parent = left

        parent.keys.pop(left_idx)
        parent.children.pop(left_idx + 1)

        if self.verbose:
            print(f"  Fusión de nodos hermanos")

    def _get_height(self, node: BPlusNode) -> int:
        """Calcula la altura del árbol"""
        if node.is_leaf:
            return 1
        return 1 + self._get_height(node.children[0])

    #UTILIDADES

    def print_tree(self, node: Optional[BPlusNode] = None, level: int = 0):
        """Visualiza el árbol"""
        if node is None:
            node = self.root
        print("  " * level + str(node))
        if not node.is_leaf:
            for child in node.children:
                self.print_tree(child, level + 1)

    def print_stats(self):
        """Imprime estadísticas del índice"""
        stats = self.get_stats()
        print("\n" + "=" * 60)
        print("ESTADÍSTICAS DEL ÍNDICE B+ TREE")
        print("=" * 60)
        for key, value in stats.items():
            if isinstance(value, dict):
                print(f"{key}:")
                for k, v in value.items():
                    print(f"  {k}: {v}")
            else:
                print(f"{key}: {value}")
        print("=" * 60)


# EJEMPLO DE USO

if __name__ == "__main__":
    print("=" * 70)
    print("B+ TREE - IMPLEMENTACIÓN MODULAR PARA PROYECTO BD2")
    print("=" * 70)

    # Crear índice clustered
    print("\n" + "=" * 70)
    print("1. ÍNDICE CLUSTERED")
    print("=" * 70)

    btree_clustered = BPlusTree(degree=3, is_clustered=True, verbose=True)

    # Insertar registros
    test_data = [
        (10, {"nombre": "Alice", "edad": 25}),
        (20, {"nombre": "Bob", "edad": 30}),
        (30, {"nombre": "Charlie", "edad": 35}),
        (40, {"nombre": "David", "edad": 40}),
        (50, {"nombre": "Eve", "edad": 45}),
        (15, {"nombre": "Frank", "edad": 28}),
        (25, {"nombre": "Grace", "edad": 32}),
    ]

    for key, data in test_data:
        btree_clustered.add(key, data)

    print("\nEstructura del árbol:")
    btree_clustered.print_tree()

    # Búsquedas
    print("\n" + "=" * 70)
    print("2. BÚSQUEDAS")
    print("=" * 70)

    results = btree_clustered.search(20)
    print(f"Búsqueda de clave 20: {results}")

    results = btree_clustered.range_search(15, 35)
    print(f"Rango [15-35]: {[(r.key, r.data) for r in results]}")

    # Eliminación
    print("\n" + "=" * 70)
    print("3. ELIMINACIÓN")
    print("=" * 70)

    btree_clustered.remove(25)
    btree_clustered.print_tree()

    # Estadísticas
    btree_clustered.print_stats()

    # Índice unclustered con duplicados
    print("\n" + "=" * 70)
    print("4. ÍNDICE UNCLUSTERED (con duplicados)")
    print("=" * 70)

    btree_unclustered = BPlusTree(degree=3, is_clustered=False, verbose=True)

    for key in [10, 20, 30, 20, 10, 40]:
        btree_unclustered.add(key, f"Data{key}")

    results = btree_unclustered.search(20)
    print(f"\nBúsqueda de clave 20 (duplicada): {results}")

    btree_unclustered.print_stats()