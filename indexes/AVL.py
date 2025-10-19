from typing import Any, List, Optional, Tuple
from .bptree_adapter import IndexInterface
from metrics import stats
import json


class _AVLNode:
    __slots__ = ("key", "vals", "left", "right", "height")

    def __init__(self, key: Any, val: Any):
        self.key = key
        self.vals = [val]
        self.left: Optional[_AVLNode] = None
        self.right: Optional[_AVLNode] = None
        self.height = 1


def _height(n: Optional[_AVLNode]) -> int:
    return n.height if n else 0


def _update(n: _AVLNode) -> None:
    n.height = 1 + max(_height(n.left), _height(n.right))


def _balance(n: _AVLNode) -> int:
    return _height(n.left) - _height(n.right)


def _rotate_left(z: _AVLNode) -> _AVLNode:
    y = z.right
    if y is None:
        return z
    T2 = y.left
    y.left = z
    z.right = T2
    _update(z)
    _update(y)
    return y


def _rotate_right(z: _AVLNode) -> _AVLNode:
    y = z.left
    if y is None:
        return z
    T3 = y.right
    y.right = z
    z.left = T3
    _update(z)
    _update(y)
    return y


class AVL(IndexInterface):
    def __init__(self, is_clustered: bool = False):
        self.root: Optional[_AVLNode] = None
        self.is_clustered = is_clustered

    def _insert(self, node: Optional[_AVLNode], key: Any, val: Any) -> _AVLNode:
        if node is None:
            return _AVLNode(key, val)
        if key == node.key:
            node.vals.append(val)
            return node
        if key < node.key:
            node.left = self._insert(node.left, key, val)
        else:
            node.right = self._insert(node.right, key, val)
        _update(node)
        bal = _balance(node)
        if bal > 1:
            if key < (node.left.key if node.left else key):
                return _rotate_right(node)
            node.left = _rotate_left(node.left)  # LR
            return _rotate_right(node)
        if bal < -1:
            if key > (node.right.key if node.right else key):
                return _rotate_left(node)
            node.right = _rotate_right(node.right)  # RL
            return _rotate_left(node)
        return node

    def add(self, key: Any, record: Any) -> bool:
        stats.inc("index.avl.add")
        self.root = self._insert(self.root, key, record)
        return True

    def _search(self, node: Optional[_AVLNode], key: Any) -> List[Any]:
        cur = node
        while cur:
            if key == cur.key:
                return list(cur.vals)
            cur = cur.left if key < cur.key else cur.right
        return []

    def search(self, key: Any) -> List[Any]:
        stats.inc("index.avl.search")
        return self._search(self.root, key)

    def _range(self, node: Optional[_AVLNode], lo: Any, hi: Any, out: List[Any]):
        if not node:
            return
        if lo < node.key:
            self._range(node.left, lo, hi, out)
        if lo <= node.key <= hi:
            out.extend(node.vals)
        if hi > node.key:
            self._range(node.right, lo, hi, out)

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        stats.inc("index.avl.range")
        if begin_key > end_key:
            begin_key, end_key = end_key, begin_key
        out: List[Any] = []
        self._range(self.root, begin_key, end_key, out)
        return out

    def _min_node(self, node: _AVLNode) -> _AVLNode:
        cur = node
        while cur.left:
            cur = cur.left
        return cur

    def _remove(self, node: Optional[_AVLNode], key: Any) -> Optional[_AVLNode]:
        if not node:
            return None
        if key < node.key:
            node.left = self._remove(node.left, key)
        elif key > node.key:
            node.right = self._remove(node.right, key)
        else:
            # eliminar todo el key
            if not node.left or not node.right:
                return node.left or node.right
            succ = self._min_node(node.right)
            node.key, node.vals = succ.key, succ.vals
            node.right = self._remove(node.right, succ.key)
        _update(node)
        bal = _balance(node)
        if bal > 1:
            if _balance(node.left) < 0:
                node.left = _rotate_left(node.left)
            return _rotate_right(node)
        if bal < -1:
            if _balance(node.right) > 0:
                node.right = _rotate_right(node.right)
            return _rotate_left(node)
        return node

    def remove(self, key: Any) -> bool:
        stats.inc("index.avl.remove")
        before = len(self.search(key))
        self.root = self._remove(self.root, key)
        return before > 0

    def get_stats(self) -> dict:
        def height(n: Optional[_AVLNode]) -> int:
            return 0 if n is None else 1 + max(height(n.left), height(n.right))
        return {
            'index_type': 'AVL',
            'clustered': self.is_clustered,
            'height': height(self.root),
        }

    # Persistencia simple JSON
    def save_idx(self, path: str) -> None:
        arr: List[Tuple[Any, List[Any]]] = []
        def inorder(n: Optional[_AVLNode]):
            if not n:
                return
            inorder(n.left)
            arr.append((n.key, n.vals))
            inorder(n.right)
        inorder(self.root)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump({'meta': {'type': 'AVL', 'clustered': self.is_clustered}, 'data': arr}, f, ensure_ascii=False)

    @classmethod
    def load_idx(cls, path: str) -> 'AVL':
        with open(path, 'r', encoding='utf-8') as f:
            blob = json.load(f)
        is_clustered = bool(blob.get('meta', {}).get('clustered', False))
        arr = blob.get('data', [])
        avl = cls(is_clustered=is_clustered)
        # construir balanced desde sorted array
        def build(a, lo, hi):
            if lo > hi:
                return None
            mid = (lo + hi) // 2
            k, vs = a[mid]
            node = _AVLNode(k, vs[0])
            node.vals = list(vs)
            node.left = build(a, lo, mid - 1)
            node.right = build(a, mid + 1, hi)
            _update(node)
            return node
        avl.root = build(arr, 0, len(arr) - 1)
        return avl