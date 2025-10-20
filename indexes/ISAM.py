"""
ISAM estático minimal que implementa IndexInterface del proyecto.
"""

from __future__ import annotations

import bisect
import json
import os
from typing import Any, Dict, List, Optional, Tuple

from .bptree_adapter import IndexInterface
from metrics import stats


class ISAM(IndexInterface):
    def __init__(self, is_clustered: bool = False, idx_path: Optional[str] = None):
        self.is_clustered = is_clustered
        self.idx_path = idx_path
        # Directorio base (ordenado por key)
        self.keys: List[Any] = []
        self.vals: List[List[Any]] = []  # lista de listas de RIDs/payloads
        # Overflow (inserciones después del build)
        self.overflow: Dict[str, List[Any]] = {}

    def _key_to_str(self, k: Any) -> str:
        return json.dumps(k, ensure_ascii=False)

    def _find_index(self, key: Any) -> int:
        stats.inc("disk.reads")  # ← AGREGAR: búsqueda binaria requiere lectura
        i = bisect.bisect_left(self.keys, key)
        return i

    # IndexInterface
    def search(self, key: Any) -> List[Any]:
        stats.inc("index.isam.search")

        with stats.timer("index.isam.search.time"):  # ← AGREGAR timer
            i = self._find_index(key)
            out: List[Any] = []

            if i < len(self.keys) and self.keys[i] == key:
                stats.inc("disk.reads")  # ← AGREGAR: leer página de datos
                out.extend(self.vals[i])

            ov = self.overflow.get(self._key_to_str(key))
            if ov:
                stats.inc("disk.reads")  # ← AGREGAR: leer overflow
                out.extend(ov)

        return out

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        stats.inc("index.isam.range")

        with stats.timer("index.isam.range.time"):  # ← AGREGAR timer
            i = bisect.bisect_left(self.keys, begin_key)
            out: List[Any] = []
            pages_read = 0

            while i < len(self.keys) and self.keys[i] <= end_key:
                stats.inc("disk.reads")  # ← AGREGAR: leer cada página en el rango
                pages_read += 1
                out.extend(self.vals[i])
                i += 1

            # overflow scan (simple)
            for k_str, lst in self.overflow.items():
                stats.inc("disk.reads")  # ← AGREGAR: leer overflow
                k = json.loads(k_str)
                if begin_key <= k <= end_key:
                    out.extend(lst)

        return out

    def add(self, key: Any, record: Any) -> bool:
        stats.inc("index.isam.add")
        stats.inc("disk.reads")  # ← AGREGAR: buscar posición

        with stats.timer("index.isam.add.time"):  # ← AGREGAR timer
            i = self._find_index(key)

            if i < len(self.keys) and self.keys[i] == key:
                self.vals[i].append(record)
                stats.inc("disk.writes")  # ← AGREGAR: actualizar página existente
                return True

            # Insert new key in base directory
            self.keys.insert(i, key)
            self.vals.insert(i, [record])
            stats.inc("disk.writes")  # ← AGREGAR: escribir nueva página

        return True

    def remove(self, key: Any) -> bool:
        stats.inc("index.isam.remove")
        stats.inc("disk.reads")  # ← AGREGAR: buscar clave

        with stats.timer("index.isam.remove.time"):  # ← AGREGAR timer
            removed = False
            i = self._find_index(key)

            if i < len(self.keys) and self.keys[i] == key:
                # elimina toda la clave del directorio
                self.keys.pop(i)
                self.vals.pop(i)
                stats.inc("disk.writes")  # ← AGREGAR: actualizar índice
                removed = True

            kstr = self._key_to_str(key)
            if kstr in self.overflow:
                del self.overflow[kstr]
                stats.inc("disk.writes")  # ← AGREGAR: actualizar overflow
                removed = True

        return removed

    def get_stats(self) -> dict:
        return {
            'index_type': 'ISAM',
            'clustered': self.is_clustered,
            'base_keys': len(self.keys),
            'overflow_keys': len(self.overflow),
        }

    # build / persistencia
    def build_from_pairs(self, pairs: List[Tuple[Any, Any]]):
        """Construye el directorio base desde (key, value) pre-colectados."""
        pairs_sorted = sorted(pairs, key=lambda kv: kv[0])
        self.keys = []
        self.vals = []
        for k, v in pairs_sorted:
            if not self.keys or k != self.keys[-1]:
                self.keys.append(k)
                self.vals.append([v])
            else:
                self.vals[-1].append(v)
        self.overflow = {}

    def save_idx(self, path: str) -> None:
        blob = {
            'meta': {'type': 'ISAM', 'clustered': self.is_clustered},
            'keys': self.keys,
            'vals': self.vals,
            'overflow': self.overflow,
        }
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(blob, f, ensure_ascii=False)

    @classmethod
    def load_idx(cls, path: str) -> 'ISAM':
        with open(path, 'r', encoding='utf-8') as f:
            blob = json.load(f)
        is_clustered = bool(blob.get('meta', {}).get('clustered', False))
        idx = cls(is_clustered=is_clustered, idx_path=path)
        idx.keys = list(blob.get('keys', []))
        idx.vals = list(blob.get('vals', []))
        idx.overflow = dict(blob.get('overflow', {}))
        return idx