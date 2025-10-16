class Record:
    def __init__(self, key, data):
        self.key = key
        self.data = data

    def __repr__(self):
        return f"Record(key={self.key}, data={self.data})"


class Node:
    def __init__(self, degree, is_leaf=False):
        self.degree = degree
        self.is_leaf = is_leaf
        self.keys = []
        self.children = []  # En hojas: Records, en internos: Nodes
        self.next = None  # Solo para nodos hoja (enlace al siguiente)

    def is_full(self):
        return len(self.keys) >= self.degree - 1


class BPlusTree:
    def __init__(self, degree):
        if degree < 3:
            raise ValueError("El grado debe ser al menos 3")
        self.degree = degree
        self.root = Node(degree, is_leaf=True)
        print(f"Árbol B+ creado con grado {degree}")

    def search(self, key):
        """Busca un registro por clave"""
        print(f"\n=== Buscando clave: {key} ===")
        return self._search(self.root, key)

    def _search(self, node, key):
        print(f"Nodo {'hoja' if node.is_leaf else 'interno'} con claves: {node.keys}")

        if node.is_leaf:
            # Buscar en el nodo hoja
            for i, k in enumerate(node.keys):
                if k == key:
                    print(f"✓ Clave {key} encontrada")
                    return node.children[i]
            print(f"✗ Clave {key} no encontrada")
            return None
        else:
            # Buscar en nodo interno
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1
            return self._search(node.children[i], key)

    def range_search(self, begin_key, end_key):
        """Búsqueda por rango"""
        print(f"\n=== Búsqueda en rango: [{begin_key}, {end_key}] ===")

        # Encontrar el primer nodo hoja que puede contener begin_key
        leaf = self._find_leaf(self.root, begin_key)
        results = []

        # Recorrer los nodos hoja enlazados
        while leaf:
            for i, key in enumerate(leaf.keys):
                if begin_key <= key <= end_key:
                    results.append(leaf.children[i])
                elif key > end_key:
                    return results
            leaf = leaf.next

        return results

    def _find_leaf(self, node, key):
        """Encuentra el nodo hoja que debería contener la clave"""
        if node.is_leaf:
            return node

        i = 0
        while i < len(node.keys) and key >= node.keys[i]:
            i += 1
        return self._find_leaf(node.children[i], key)

    def add(self, record):
        """Inserta un nuevo registro"""
        print(f"\n=== Insertando registro: key={record.key} ===")

        # Si la raíz está llena, dividirla primero
        if self.root.is_full():
            old_root = self.root
            self.root = Node(self.degree, is_leaf=False)
            self.root.children.append(old_root)
            self._split_child(self.root, 0)

        self._insert_non_full(self.root, record)

    def _insert_non_full(self, node, record):
        """Inserta en un nodo que no está lleno"""
        if node.is_leaf:
            # Insertar en orden en el nodo hoja
            i = 0
            while i < len(node.keys) and node.keys[i] < record.key:
                i += 1

            node.keys.insert(i, record.key)
            node.children.insert(i, record)
            print(f"Insertado en hoja: {record.key}")
        else:
            # Encontrar el hijo apropiado
            i = 0
            while i < len(node.keys) and record.key >= node.keys[i]:
                i += 1

            # Si el hijo está lleno, dividirlo
            if node.children[i].is_full():
                self._split_child(node, i)
                # Después del split, decidir en qué hijo insertar
                if record.key >= node.keys[i]:
                    i += 1

            self._insert_non_full(node.children[i], record)

    def _split_child(self, parent, index):
        """Divide un nodo hijo que está lleno"""
        full_node = parent.children[index]
        mid = len(full_node.keys) // 2

        # Crear nuevo nodo
        new_node = Node(self.degree, is_leaf=full_node.is_leaf)

        if full_node.is_leaf:
            # División de nodo hoja
            new_node.keys = full_node.keys[mid:]
            new_node.children = full_node.children[mid:]

            full_node.keys = full_node.keys[:mid]
            full_node.children = full_node.children[:mid]

            # Enlazar los nodos hoja
            new_node.next = full_node.next
            full_node.next = new_node

            # La clave que sube es la primera del nuevo nodo
            promoted_key = new_node.keys[0]
        else:
            # División de nodo interno
            new_node.keys = full_node.keys[mid + 1:]
            new_node.children = full_node.children[mid + 1:]

            promoted_key = full_node.keys[mid]

            full_node.keys = full_node.keys[:mid]
            full_node.children = full_node.children[:mid + 1]

        # Insertar la clave promocionada y el nuevo hijo en el padre
        parent.keys.insert(index, promoted_key)
        parent.children.insert(index + 1, new_node)

        print(f"Nodo dividido. Clave promocionada: {promoted_key}")

    def remove(self, key):
        """Elimina un registro por clave"""
        print(f"\n=== Eliminando clave: {key} ===")
        self._remove(self.root, key)

        # Si la raíz quedó vacía (solo tiene un hijo), bajar el nivel
        if not self.root.is_leaf and len(self.root.keys) == 0:
            self.root = self.root.children[0]
            print("Raíz vacía, reduciendo altura del árbol")

    def _remove(self, node, key):
        """Elimina recursivamente"""
        if node.is_leaf:
            # Eliminar de la hoja
            if key in node.keys:
                idx = node.keys.index(key)
                node.keys.pop(idx)
                node.children.pop(idx)
                print(f"Clave {key} eliminada de la hoja")
            else:
                print(f"Clave {key} no encontrada")
        else:
            # Encontrar el hijo apropiado
            i = 0
            while i < len(node.keys) and key >= node.keys[i]:
                i += 1

            self._remove(node.children[i], key)

            # Rebalancear si es necesario (implementación simplificada)
            # En una implementación completa, aquí iría la lógica de rebalanceo

    def print_tree(self, node=None, level=0):
        """Imprime el árbol para visualización"""
        if node is None:
            node = self.root

        indent = "  " * level
        node_type = "HOJA" if node.is_leaf else "INTERNO"
        print(f"{indent}[{node_type}] Keys: {node.keys}")

        if not node.is_leaf:
            for child in node.children:
                self.print_tree(child, level + 1)


# ===== PRUEBAS =====
if __name__ == "__main__":
    print("=" * 60)
    print("PRUEBAS DEL ÁRBOL B+")
    print("=" * 60)

    # Crear árbol con grado 3 (max 2 claves por nodo)
    tree = BPlusTree(degree=3)

    # Insertar registros
    print("\n" + "=" * 60)
    print("INSERTANDO REGISTROS")
    print("=" * 60)
    for i in [5, 15, 25, 35, 45, 10, 20, 30, 40]:
        tree.add(Record(i, f"Data{i}"))

    # Visualizar el árbol
    print("\n" + "=" * 60)
    print("ESTRUCTURA DEL ÁRBOL")
    print("=" * 60)
    tree.print_tree()

    # Buscar registros
    print("\n" + "=" * 60)
    print("BÚSQUEDAS")
    print("=" * 60)
    result = tree.search(25)
    if result:
        print(f"Resultado: {result}")

    result = tree.search(100)
    if result:
        print(f"Resultado: {result}")

    # Búsqueda por rango
    print("\n" + "=" * 60)
    print("BÚSQUEDA POR RANGO")
    print("=" * 60)
    results = tree.range_search(15, 35)
    print(f"Registros en rango [15, 35]:")
    for r in results:
        print(f"  {r}")

    # Eliminar
    print("\n" + "=" * 60)
    print("ELIMINACIÓN")
    print("=" * 60)
    tree.remove(25)

    print("\nEstructura después de eliminar 25:")
    tree.print_tree()

    # Verificar que ya no existe
    result = tree.search(25)
    print(f"\nBúsqueda de 25 después de eliminar: {result}")