"""Índice de hashing extensible con buckets dinámicos.

Implementa un índice basado en hashing extensible que soporta:
- Búsqueda por clave con acceso directo mediante función hash.
- Crecimiento dinámico del directorio y división de buckets cuando se llenan.
- Persistencia en JSON para almacenamiento y recuperación del índice.
- Registro de métricas de lectura/escritura y tiempos de operación.
"""
from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .bptree_adapter import IndexInterface
from metrics import stats


class _Bucket:
    """Bucket de almacenamiento para el índice hash extensible.
    
    Cada bucket mantiene un mapeo de claves a listas de registros y
    tiene una profundidad local que determina su expansión.
    """
    def __init__(self, local_depth: int, capacity: int = 8):
        self.local_depth = local_depth
        self.capacity = capacity
        self.map: Dict[Any, List[Any]] = {}

    def size(self) -> int:
        # total stored values across keys
        return sum(len(vs) for vs in self.map.values())

    def is_full(self) -> bool:
        return self.size() >= self.capacity

    def add(self, key: Any, value: Any) -> None:
        self.map.setdefault(key, []).append(value)

    def search(self, key: Any) -> List[Any]:
        return list(self.map.get(key, []))

    def remove(self, key: Any) -> bool:
        if key in self.map:
            del self.map[key]
            return True
        return False


class ExtHashing(IndexInterface):
    """Índice de hashing extensible con crecimiento dinámico.
    
    Mantiene un directorio de punteros a buckets y soporta división
    de buckets cuando se llenan, duplicando el directorio si es necesario.
    """
    def __init__(self, is_clustered: bool = False, global_depth: int = 2, bucket_capacity: int = 8):
        self.is_clustered = is_clustered
        self.global_depth = max(1, int(global_depth))
        self.bucket_capacity = int(bucket_capacity)
        self.buckets: List[_Bucket] = []
        self.directory: List[int] = []
        self._init_empty()

    def _init_empty(self) -> None:
        """Inicializa el índice vacío con buckets y directorio inicial."""
        self.buckets = []
        num = 1 << self.global_depth
        for _ in range(num):
            self.buckets.append(_Bucket(local_depth=self.global_depth, capacity=self.bucket_capacity))
        self.directory = list(range(num))

    def _hash(self, key: Any) -> int:
        h = hash(json.dumps(key, ensure_ascii=False))
        mask = (1 << self.global_depth) - 1
        return h & mask

    def _bucket_index_for(self, key: Any) -> int:
        """Determina el índice del bucket para una clave dada."""
        hv = self._hash(key)
        return self.directory[hv]

    def search(self, key: Any) -> List[Any]:
        """Busca todos los registros asociados a una clave."""
        stats.inc("index.hash.search")
        stats.inc("disk.reads")

        with stats.timer("index.hash.search.time"):
            bidx = self._bucket_index_for(key)
            result = self.buckets[bidx].search(key)

        return result

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        """Búsqueda por rango no soportada eficientemente en hash."""
        stats.inc("index.hash.range")
        return []

    def add(self, key: Any, record: Any) -> bool:
        """Agrega un registro al índice hash.
        
        Si el bucket está lleno, lo divide y redistribuye las entradas.
        """
        stats.inc("index.hash.add")
        stats.inc("disk.reads")

        with stats.timer("index.hash.add.time"):
            bidx = self._bucket_index_for(key)
            bucket = self.buckets[bidx]

            if not bucket.is_full() or key in bucket.map:
                bucket.add(key, record)
                stats.inc("disk.writes")
                return True

            self._split_bucket(bidx)

            bidx2 = self._bucket_index_for(key)
            self.buckets[bidx2].add(key, record)
            stats.inc("disk.writes")

        return True

    def remove(self, key: Any) -> bool:
        """Elimina todas las entradas asociadas a una clave."""
        stats.inc("index.hash.remove")
        stats.inc("disk.reads")

        with stats.timer("index.hash.remove.time"):
            bidx = self._bucket_index_for(key)
            result = self.buckets[bidx].remove(key)

            if result:
                stats.inc("disk.writes")

        return result

    def get_stats(self) -> dict:
        return {
            "index_type": "HASH",
            "clustered": self.is_clustered,
            "global_depth": self.global_depth,
            "buckets": len(self.buckets),
            "directory_entries": len(self.directory),
        }

    def _split_bucket(self, bidx: int) -> None:
        """Divide un bucket lleno y redistribuye sus entradas."""
        stats.inc("disk.writes", 2)

        bucket = self.buckets[bidx]
        if bucket.local_depth == self.global_depth:
            self._double_directory()

        # create new bucket
        new_depth = bucket.local_depth + 1
        bucket.local_depth = new_depth
        new_bucket = _Bucket(local_depth=new_depth, capacity=self.bucket_capacity)
        self.buckets.append(new_bucket)
        new_idx = len(self.buckets) - 1

        # rewire directory entries
        bit = 1 << (new_depth - 1)
        for i, idx in enumerate(self.directory):
            if idx == bidx:
                if (i & bit) != 0:
                    self.directory[i] = new_idx

        # redistribute entries
        items: List[tuple[Any, Any]] = []
        for k, vs in list(bucket.map.items()):
            for v in vs:
                items.append((k, v))
        bucket.map.clear()

        for k, v in items:
            stats.inc("disk.reads")
            idx = self._bucket_index_for(k)
            self.buckets[idx].add(k, v)

    def _double_directory(self) -> None:
        """Duplica el tamaño del directorio cuando es necesario."""
        stats.inc("disk.writes")
        old_dir = self.directory
        self.global_depth += 1
        self.directory = old_dir + old_dir[:]

    def save_idx(self, path: str) -> None:
        """Guarda el índice hash en un archivo JSON."""
        b_arr = []
        for b in self.buckets:
            enc_map: Dict[str, List[Any]] = {}
            for k, vs in b.map.items():
                enc_map[json.dumps(k, ensure_ascii=False)] = vs
            b_arr.append({"local_depth": b.local_depth, "map": enc_map})

        blob = {
            "meta": {
                "type": "HASH",
                "clustered": self.is_clustered,
                "global_depth": self.global_depth,
                "bucket_capacity": self.bucket_capacity,
            },
            "buckets": b_arr,
            "directory": list(self.directory)
        }

        with open(path, "w", encoding="utf-8") as f:
            json.dump(blob, f, ensure_ascii=False)

    @classmethod
    def load_idx(cls, path: str) -> "ExtHashing":
        """Carga el índice hash desde un archivo JSON."""
        with open(path, "r", encoding="utf-8") as f:
            blob = json.load(f)
        meta = blob.get("meta", {})
        inst = cls(
            is_clustered=bool(meta.get("clustered", False)),
            global_depth=int(meta.get("global_depth", 2)),
            bucket_capacity=int(meta.get("bucket_capacity", 8)),
        )
        inst.buckets = []
        for b in blob.get("buckets", []):
            bk = _Bucket(local_depth=int(b.get("local_depth", inst.global_depth)), capacity=inst.bucket_capacity)
            dec_map: Dict[Any, List[Any]] = {}
            for k_str, vs in b.get("map", {}).items():
                dec_map[json.loads(k_str)] = list(vs)
            bk.map = dec_map
            inst.buckets.append(bk)
        inst.directory = list(blob.get("directory", list(range(1 << inst.global_depth))))
        if not inst.buckets:
            inst._init_empty()
        return inst