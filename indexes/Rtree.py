from __future__ import annotations

import json
import math
from typing import Any, Dict, List, Optional, Tuple

try:
    # Pylance: rtree may not be installed in all environments
    from rtree import index as rtree_index  # type: ignore[import-not-found]
except Exception:  # pragma: no cover - entorno sin rtree
    rtree_index = None  # type: ignore

from .bptree_adapter import IndexInterface
from metrics import stats


class RTreeIndex(IndexInterface):
    """Índice espacial R-Tree compatible con IndexInterface.

    Almacena puntos n-dimensionales (2D/3D típicamente) asociados a RIDs.
    Persistencia en JSON; el árbol se reconstruye al cargar.

    Operaciones extra para espaciales:
    - range_search_radius(point, radius) → rids en ese radio
    - knn(point, k) → rids de los k vecinos más cercanos
    """

    def __init__(self, dimensions: int = 2):
        if dimensions < 2:
            raise ValueError("RTreeIndex requiere al menos 2 dimensiones")
        self.dimensions = int(dimensions)
        # id -> (coords, rid)
        self._points: Dict[int, Tuple[List[float], Any]] = {}
        self._next_id = 1
        # índice en memoria (rtree) si está disponible
        self._rtree = None
        if rtree_index is not None:
            p = rtree_index.Property()
            p.dimension = self.dimensions
            # usamos índice en memoria; persistimos vía JSON
            self._rtree = rtree_index.Index(properties=p)

    # --------- IndexInterface ---------
    def search(self, key: Any) -> List[Any]:
        """Búsqueda por igualdad exacta de coordenadas [x,y(,z)]."""
        stats.inc("index.rtree.search")
        coords = self._coerce_point(key)
        # encontrar todos con coords exactas
        out: List[Any] = []
        for _, (pt, rid) in self._points.items():
            if self._eq_coords(pt, coords):
                out.append(rid)
        return out

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        """No aplica semánticamente para RTree (usamos range_search_radius)."""
        stats.inc("index.rtree.range_unsupported")
        return []

    def add(self, key: Any, record: Any) -> bool:
        stats.inc("index.rtree.add")
        coords = self._coerce_point(key)
        pid = self._next_id
        self._next_id += 1
        self._points[pid] = (coords, record)
        if self._rtree is not None:
            bbox = self._bbox(coords)
            self._rtree.insert(pid, bbox)
        return True

    def remove(self, key: Any) -> bool:
        stats.inc("index.rtree.remove")
        coords = self._coerce_point(key)
        to_del: List[int] = [pid for pid, (pt, _) in self._points.items() if self._eq_coords(pt, coords)]
        ok = False
        for pid in to_del:
            pt, _ = self._points.get(pid, (None, None))  # type: ignore
            if pt is None:
                continue
            if self._rtree is not None:
                self._rtree.delete(pid, self._bbox(pt))
            # borrar del diccionario en memoria
            del self._points[pid]
            ok = True
        return ok

    def get_stats(self) -> dict:
        return {
            "index_type": "RTREE",
            "dimensions": self.dimensions,
            "points": len(self._points),
        }

    # --------- Operaciones espaciales ---------
    def range_search_radius(self, center: List[float], radius: float) -> List[Any]:
        stats.inc("index.rtree.range_radius")
        c = self._coerce_point(center)
        out: List[Any] = []
        if self._rtree is None:
            # sin rtree: filtrar lineal
            for pt, rid in self._points.values():
                if self._dist(c, pt) <= radius:
                    out.append(rid)
            return out
        # usar bbox para candidatos
        candidates = list(self._rtree.intersection(self._bbox_for_radius(c, radius)))
        for pid in candidates:
            pt, rid = self._points.get(pid, (None, None))  # type: ignore
            if pt is None:
                continue
            if self._dist(c, pt) <= radius:
                out.append(rid)
        return out

    def knn(self, center: List[float], k: int) -> List[Any]:
        stats.inc("index.rtree.knn")
        c = self._coerce_point(center)
        if k <= 0:
            return []
        if self._rtree is None:
            # ordenar linealmente por distancia
            arr = sorted(((self._dist(c, pt), rid) for pt, rid in self._points.values()), key=lambda x: x[0])
            return [rid for _, rid in arr[:k]]
        # rtree nearest
        q = self._point_bbox(c)
        ids = list(self._rtree.nearest(q, num_results=k))
        arr: List[Tuple[float, Any]] = []
        for pid in ids:
            pt, rid = self._points.get(pid, (None, None))  # type: ignore
            if pt is None:
                continue
            arr.append((self._dist(c, pt), rid))
        arr.sort(key=lambda x: x[0])
        return [rid for _, rid in arr[:k]]

    # --------- Persistencia JSON ---------
    def save_idx(self, path: str) -> None:
        blob = {
            "meta": {"type": "RTREE", "dimensions": self.dimensions, "next_id": self._next_id},
            "points": [
                {"id": pid, "coords": coords, "rid": rid}
                for pid, (coords, rid) in self._points.items()
            ],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False)

    @classmethod
    def load_idx(cls, path: str) -> "RTreeIndex":
        with open(path, "r", encoding="utf-8") as f:
            blob = json.load(f)
        dims = int(blob.get("meta", {}).get("dimensions", 2))
        inst = cls(dimensions=dims)
        inst._next_id = int(blob.get("meta", {}).get("next_id", 1))
        for p in blob.get("points", []):
            pid = int(p.get("id"))
            coords = [float(x) for x in p.get("coords", [])]
            rid = p.get("rid")
            inst._points[pid] = (coords, rid)
            if inst._rtree is not None:
                inst._rtree.insert(pid, inst._bbox(coords))
        return inst

    # --------- Helpers ---------
    def _coerce_point(self, v: Any) -> List[float]:
        if isinstance(v, (list, tuple)) and len(v) == self.dimensions:
            return [float(x) for x in v]
        raise ValueError(f"Se esperaban {self.dimensions} dimensiones")

    def _eq_coords(self, a: List[float], b: List[float]) -> bool:
        # igualdad exacta; en escenarios reales se usaría tolerancia
        return all(float(x) == float(y) for x, y in zip(a, b))

    def _bbox(self, pt: List[float]) -> Tuple[float, ...]:
        if self.dimensions == 2:
            x, y = pt
            return (x, y, x, y)
        elif self.dimensions == 3:
            x, y, z = pt
            return (x, y, z, x, y, z)
        else:
            # rtree soporta N-D con pares min/max
            return tuple(pt + pt)

    def _point_bbox(self, pt: List[float]) -> Tuple[float, ...]:
        return self._bbox(pt)

    def _bbox_for_radius(self, c: List[float], r: float) -> Tuple[float, ...]:
        if self.dimensions == 2:
            x, y = c
            return (x - r, y - r, x + r, y + r)
        elif self.dimensions == 3:
            x, y, z = c
            return (x - r, y - r, z - r, x + r, y + r, z + r)
        else:
            # hipercubo sencillo
            mins = [v - r for v in c]
            maxs = [v + r for v in c]
            return tuple(mins + maxs)

    def _dist(self, a: List[float], b: List[float]) -> float:
        return math.sqrt(sum((x - y) ** 2 for x, y in zip(a, b)))
