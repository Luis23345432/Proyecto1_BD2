from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from .bptree_adapter import IndexInterface
from metrics import stats


class _Bucket:
    def __init__(self, local_depth: int, capacity: int = 8):
        self.local_depth = local_depth
        self.capacity = capacity
        # map key -> list[record]
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
    """Extendible Hashing implementing IndexInterface with JSON persistence."""

    def __init__(self, is_clustered: bool = False, global_depth: int = 2, bucket_capacity: int = 8):
        self.is_clustered = is_clustered
        self.global_depth = max(1, int(global_depth))
        self.bucket_capacity = int(bucket_capacity)
        self.buckets: List[_Bucket] = []
        # directory: list of bucket indices of length 2^global_depth
        self.directory: List[int] = []
        # init
        self._init_empty()

    # initialization helpers
    def _init_empty(self) -> None:
        self.buckets = []
        # create 2^D buckets initially
        num = 1 << self.global_depth
        for _ in range(num):
            self.buckets.append(_Bucket(local_depth=self.global_depth, capacity=self.bucket_capacity))
        self.directory = list(range(num))

    # hashing helpers
    def _hash(self, key: Any) -> int:
        h = hash(json.dumps(key, ensure_ascii=False))
        mask = (1 << self.global_depth) - 1
        return h & mask

    def _bucket_index_for(self, key: Any) -> int:
        hv = self._hash(key)
        return self.directory[hv]

    # IndexInterface
    def search(self, key: Any) -> List[Any]:
        stats.inc("index.hash.search")
        stats.inc("disk.reads")  # ← AGREGAR: leer bucket del directorio

        with stats.timer("index.hash.search.time"):  # ← AGREGAR timer
            bidx = self._bucket_index_for(key)
            result = self.buckets[bidx].search(key)

        return result

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        stats.inc("index.hash.range")
        # Not supported efficiently
        return []

    def add(self, key: Any, record: Any) -> bool:
        stats.inc("index.hash.add")
        stats.inc("disk.reads")  # ← AGREGAR: leer bucket

        with stats.timer("index.hash.add.time"):  # ← AGREGAR timer
            bidx = self._bucket_index_for(key)
            bucket = self.buckets[bidx]

            if not bucket.is_full() or key in bucket.map:
                bucket.add(key, record)
                stats.inc("disk.writes")  # ← AGREGAR: escribir bucket
                return True

            # split if full and no room
            self._split_bucket(bidx)

            # retry insert
            bidx2 = self._bucket_index_for(key)
            self.buckets[bidx2].add(key, record)
            stats.inc("disk.writes")  # ← AGREGAR: escribir después de split

        return True

    def remove(self, key: Any) -> bool:
        stats.inc("index.hash.remove")
        stats.inc("disk.reads")  # ← AGREGAR: leer bucket

        with stats.timer("index.hash.remove.time"):  # ← AGREGAR timer
            bidx = self._bucket_index_for(key)
            result = self.buckets[bidx].remove(key)

            if result:
                stats.inc("disk.writes")  # ← AGREGAR: escribir cambios

        return result

    def get_stats(self) -> dict:
        return {
            "index_type": "HASH",
            "clustered": self.is_clustered,
            "global_depth": self.global_depth,
            "buckets": len(self.buckets),
            "directory_entries": len(self.directory),
        }

    # splitting / directory doubling
    def _split_bucket(self, bidx: int) -> None:
        stats.inc("disk.writes", 2)  # ← AGREGAR: split implica 2 escrituras (bucket viejo + nuevo)

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
            stats.inc("disk.reads")  # ← AGREGAR: leer durante redistribución
            idx = self._bucket_index_for(k)
            self.buckets[idx].add(k, v)

    def _double_directory(self) -> None:
        stats.inc("disk.writes")  # ← AGREGAR: duplicar directorio requiere escritura
        old_dir = self.directory
        self.global_depth += 1
        self.directory = old_dir + old_dir[:]

    # persistence (JSON)
    def save_idx(self, path: str) -> None:
        # Properly encode bucket maps
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
        # if buckets list was empty (corrupt), re-init
        if not inst.buckets:
            inst._init_empty()
        return inst