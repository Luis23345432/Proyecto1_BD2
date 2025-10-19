"""
B+ Tree Index Implementation
Implementación modular lista para integración con sistema de base de datos
Autor: Luis Lopez

Paso 4: IndexInterface (clase abstracta)
Paso 5: B+ Tree completo con split/merge/redistribución, soporte clustered/unclustered y persistencia .idx
"""

import math
import json
from typing import Any, List, Optional, Tuple, Dict
from abc import ABC, abstractmethod
from metrics import stats


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


# NOTA: Este módulo define sólo el índice. La gestión de datos físicos se realiza en otros módulos.


#ÍNDICE B+ TREE

class IndexEntry:
    """Entrada de hoja del índice.

    - Para índices clustered: vals contiene RIDs (tuplas como (page_id, offset/slot)).
    - Para índices unclustered: vals contiene payloads (los datos del registro).
    """

    def __init__(self, key: Any, vals: Optional[List[Any]] = None):
        self.key = key
        self.vals: List[Any] = list(vals or [])

    def __repr__(self):
        return f"({self.key} → {len(self.vals)} vals)"


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
                 is_clustered: bool = False,
                 verbose: bool = False,
                 idx_path: Optional[str] = None):
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
        self.idx_path = idx_path

        # Contadores para métricas
        self.search_count = 0
        self.insert_count = 0
        self.delete_count = 0
        self.split_count = 0
        self.merge_count = 0

        if self.verbose:
            print(f"B+ Tree {'CLUSTERED' if is_clustered else 'UNCLUSTERED'} creado (grado={degree})")

    #OPERACIONES PRINCIPALES

    def search(self, key: Any) -> List[Any]:
        """Busca todos los valores asociados a la clave.
        - Clustered: devuelve lista de RIDs
        - Unclustered: devuelve lista de payloads
        """
        stats.inc("index.btree.search")
        self.search_count += 1

        if self.verbose:
            print(f"\n>>> SEARCH key={key}")

        entry = self._find_entry(self.root, key)
        if not entry:
            if self.verbose:
                print(f"  Clave {key} no encontrada")
            return []

        results = list(entry.vals)

        if self.verbose:
            print(f"  Encontrados {len(results)} registro(s)")

        return results

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        """Busca todos los valores en el rango [begin_key, end_key]."""
        stats.inc("index.btree.range")
        self.search_count += 1

        if self.verbose:
            print(f"\n>>> RANGE SEARCH [{begin_key}, {end_key}]")

        leaf = self._find_leaf(self.root, begin_key)
        results = []

        while leaf:
            for i, key in enumerate(leaf.keys):
                if begin_key <= key <= end_key:
                    entry = leaf.children[i]
                    results.extend(entry.vals)
                elif key > end_key:
                    if self.verbose:
                        print(f"  Encontrados {len(results)} registros")
                    return results
            leaf = leaf.next

        if self.verbose:
            print(f"  Encontrados {len(results)} registros")

        return results

    def add(self, key: Any, value: Any) -> bool:
        stats.inc("index.btree.add")
        """
        Inserta un nuevo valor para la clave.
        Args:
            key: Clave del registro
            value: Si el índice es clustered, se espera un RID (p.ej. (page_id, offset/slot)).
                   Si es unclustered, se espera el payload (dict, tuple, etc.).
        Returns:
            True si se insertó correctamente
        """
        self.insert_count += 1

        if self.verbose:
            print(f"\n>>> INSERT key={key}")

        # 1. Si ya existe la clave en la hoja destino, agregamos al entry (permitir duplicados)
        existing_entry = self._find_entry(self.root, key)
        if existing_entry and existing_entry.key == key:
            existing_entry.vals.append(value)
            return True

        # 2. Insertar en el índice (para clustered o primer valor unclustered)
        if self.root.is_full():
            old_root = self.root
            self.root = BPlusNode(self.degree, is_leaf=False)
            self.root.children.append(old_root)
            old_root.parent = self.root
            self._split_child(self.root, 0)

        self._insert_non_full(self.root, IndexEntry(key, [value]))
        return True

    def remove(self, key: Any) -> bool:
        stats.inc("index.btree.remove")
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
            # Si existe misma clave en la hoja: fusionar valores
            if i < len(node.keys) and node.keys[i] == entry.key:
                node.children[i].vals.extend(entry.vals)
            else:
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
                # Eliminar toda la clave y sus valores
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


    # Persistencia .idx (opcional)
    def save_idx(self, path: str) -> None:
        def node_to_dict(node: BPlusNode) -> Dict[str, Any]:
            if node.is_leaf:
                vals_list = [child.vals for child in node.children]
                return {"leaf": True, "keys": node.keys, "vals": vals_list}
            else:
                return {
                    "leaf": False,
                    "keys": node.keys,
                    "children": [node_to_dict(c) for c in node.children],
                }

        data = node_to_dict(self.root)
        meta = {"is_clustered": self.is_clustered, "degree": self.degree}
        blob = {"meta": meta, "tree": data}
        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False)

    @classmethod
    def load_idx(cls, path: str, verbose: bool = False) -> "BPlusTree":
        with open(path, "r", encoding="utf-8") as f:
            blob = json.load(f)
        meta = blob.get("meta", {})
        degree = int(meta.get("degree", 3))
        is_clustered = bool(meta.get("is_clustered", False))
        tree = cls(degree=degree, is_clustered=is_clustered, verbose=verbose)

        def dict_to_node(d: Dict[str, Any], parent: Optional[BPlusNode] = None) -> BPlusNode:
            node = BPlusNode(degree=degree, is_leaf=bool(d.get("leaf", False)))
            node.keys = list(d.get("keys", []))
            node.parent = parent
            if node.is_leaf:
                vals_list = d.get("vals", [])
                node.children = [IndexEntry(k, v) for k, v in zip(node.keys, vals_list)]
            else:
                node.children = [dict_to_node(cd, node) for cd in d.get("children", [])]
                # apuntar next en hojas (reconstrucción simple): enlazar leaves in-order
            return node

        tree.root = dict_to_node(blob.get("tree", {}))

        # Reconstruir punteros next en hojas
        leaves: List[BPlusNode] = []
        def collect_leaves(n: BPlusNode):
            if n.is_leaf:
                leaves.append(n)
            else:
                for c in n.children:
                    collect_leaves(c)
        collect_leaves(tree.root)
        for i in range(len(leaves) - 1):
            leaves[i].next = leaves[i + 1]

        return tree