"""
Clase Table (Pasos 12):
- Administra data.dat mediante DataFile
- Mantiene un índice por columna (cuando el esquema lo solicita)
- Inserción sincroniza índices
"""

from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional, Tuple

from core.schema import TableSchema
from core.types import convert_value
from core.records import Record
from indexes import BPlusTree
from indexes.ISAM import ISAM
from indexes.AVL import AVL
from indexes.ExtHashing import ExtHashing
from indexes.Rtree import RTreeIndex
from datafile import DataFile
from metrics import stats


class Table:
    def __init__(self, base_dir: str, schema: TableSchema, page_size: int = 4096):
        self.base_dir = os.path.abspath(base_dir)
        self.schema = schema
        self.page_size = page_size
        os.makedirs(self.base_dir, exist_ok=True)
        self.data_path = os.path.join(self.base_dir, "data.dat")
        self.schema_path = os.path.join(self.base_dir, "schema.json")
        self.index_dir = os.path.join(self.base_dir, "indexes")
        os.makedirs(self.index_dir, exist_ok=True)

        # almacenamiento
        self.datafile = DataFile(self.data_path, page_size=self.page_size)

        # índices por columna
        self.indexes: Dict[str, BPlusTree] = {}
        for col_name, idx_type in self.schema.indexes.items():
            idx_path = os.path.join(self.index_dir, f"{col_name}.idx")
            idx_obj = None
            if os.path.exists(idx_path):
                try:
                    if idx_type.name.lower() == 'btree':
                        idx_obj = BPlusTree.load_idx(idx_path)
                    elif idx_type.name.lower() == 'isam':
                        idx_obj = ISAM.load_idx(idx_path)
                    elif idx_type.name.lower() == 'hash':
                        idx_obj = ExtHashing.load_idx(idx_path)
                    elif idx_type.name.lower() == 'rtree':
                        idx_obj = RTreeIndex.load_idx(idx_path)
                    else:
                        idx_obj = AVL.load_idx(idx_path)
                except Exception:
                    idx_obj = None
            if idx_obj is None:
                is_clustered = self.schema.get_column(col_name).primary_key
                if idx_type.name.lower() == 'btree':
                    idx_obj = BPlusTree(degree=5, is_clustered=is_clustered)
                elif idx_type.name.lower() == 'isam':
                    idx_obj = ISAM(is_clustered=is_clustered)
                elif idx_type.name.lower() == 'hash':
                    idx_obj = ExtHashing(is_clustered=is_clustered)
                elif idx_type.name.lower() == 'rtree':
                    # RTree no distingue clustered; asumimos puntos en columna ARRAY_FLOAT
                    idx_obj = RTreeIndex(dimensions=self._infer_dimensions_for(col_name))
                else:
                    idx_obj = AVL(is_clustered=is_clustered)
            self.indexes[col_name] = idx_obj

        # guardar schema si no existe
        if not os.path.exists(self.schema_path):
            self.schema.save(self.schema_path)

    # Persistencia de índices
    def _save_indexes(self):
        for col_name, tree in self.indexes.items():
            idx_path = os.path.join(self.index_dir, f"{col_name}.idx")
            tree.save_idx(idx_path)

    def _infer_dimensions_for(self, column: str) -> int:
        try:
            col = self.schema.get_column(column)
            vtype = col.col_type
            if vtype.name == 'ARRAY_FLOAT':
                # por defecto 2D si no hay datos todavía
                return 2
        except Exception:
            pass
        return 2

    def insert(self, values: Dict[str, Any]) -> Tuple[int, int]:
        stats.inc("table.insert.calls")
        with stats.timer("table.insert.time"):
            print(f"1. DEBUG values recibidos: {values}")
            print(f"1. DEBUG ubicacion tipo: {type(values.get('ubicacion'))}")

            # ← AGREGAR ESTO
            print(f"DEBUG schema.columns:")
            for col in self.schema.columns:
                print(f"  - {col.name}: type={col.col_type}, length={col.length}")

            rec = Record(self.schema, values)

            print(f"2. DEBUG rec.values: {rec.values}")
            print(f"2. DEBUG ubicacion tipo después Record: {type(rec.values.get('ubicacion'))}")

            rec_dict = rec.to_dict()

            print(f"3. DEBUG rec.to_dict(): {rec_dict}")
            print(f"3. DEBUG ubicacion tipo después to_dict: {type(rec_dict.get('ubicacion'))}")

            rid = self.datafile.insert_clustered(rec_dict)

            print(f"4. DEBUG después de insert_clustered, rid: {rid}")

            # actualizar índices
            for col in self.schema.columns:
                if col.name in self.indexes:
                    key = rec.values[col.name]
                    if key is None:
                        continue
                    tree = self.indexes[col.name]

                    print(f"5. DEBUG indexando {col.name}, key: {key}, tipo: {type(key)}")

                    # Para RTree, la clave debe ser un punto [x,y,(z)]
                    try:
                        if isinstance(tree, RTreeIndex):
                            # aseguramos lista de floats
                            if not isinstance(key, (list, tuple)):
                                # si el valor viene como string "x,y"
                                if isinstance(key, str):
                                    parts = [p.strip() for p in key.split(',')]
                                    key = [float(p) for p in parts]

                            print(f"6. DEBUG antes de tree.add, key: {key}, tipo: {type(key)}")
                            tree.add(key, rid)
                        else:
                            tree.add(key, rid)
                    except Exception as e:
                        print(f"ERROR en tree.add: {e}")
                        # si falla, no rompemos la inserción de la tabla
                        tree.add(key, rid)
            self._save_indexes()
            return rid
    def _pick_index(self, column: str) -> Optional[BPlusTree]:
        return self.indexes.get(column)

    def search(self, column: str, key: Any) -> List[Dict[str, Any]]:
        stats.inc("table.search.calls")
        with stats.timer("table.search.time"):
            # Coerce key to column type to keep comparisons consistent
            try:
                col = self.schema.get_column(column)
                key = convert_value(col.col_type, key)
            except Exception:
                pass
            tree = self._pick_index(column)
            if not tree:
                # sin índice: escaneo de todas las páginas
                out: List[Dict[str, Any]] = []
                try:
                    pc = self.datafile.page_count()
                except Exception:
                    return []
                for pid in range(pc):
                    page = self.datafile.read_page(pid)
                    recs = page.iter_records()
                    for r in recs:
                        if r.get(column) == key:
                            out.append(r)
                return out
            rids = tree.search(key)
            return [self.fetch_by_rid(rid) for rid in rids]

    def range_search(self, column: str, begin_key: Any, end_key: Any) -> List[Dict[str, Any]]:
        stats.inc("table.range.calls")
        with stats.timer("table.range.time"):
            try:
                col = self.schema.get_column(column)
                begin_key = convert_value(col.col_type, begin_key)
                end_key = convert_value(col.col_type, end_key)
            except Exception:
                pass
            tree = self._pick_index(column)
            if not tree:
                return []
            # Si es RTREE, range_search (begin,end) no aplica; devolvemos [] y dejamos a API/SQL usar métodos espaciales dedicados
            if isinstance(tree, RTreeIndex):
                rids = []
            else:
                rids = tree.range_search(begin_key, end_key)
            return [self.fetch_by_rid(rid) for rid in rids]

    # Búsqueda espacial por radio
    def range_radius(self, column: str, center: Any, radius: float) -> List[Dict[str, Any]]:
        tree = self._pick_index(column)
        if not tree or not isinstance(tree, RTreeIndex):
            return []
        # coerce center to list[float]
        if not isinstance(center, (list, tuple)):
            if isinstance(center, str):
                parts = [p.strip() for p in center.split(',')]
                center = [float(p) for p in parts]
        rids = tree.range_search_radius(center, float(radius))
        return [self.fetch_by_rid(rid) for rid in rids]

    # K vecinos más cercanos
    def knn(self, column: str, center: Any, k: int) -> List[Dict[str, Any]]:
        tree = self._pick_index(column)
        if not tree or not isinstance(tree, RTreeIndex):
            return []
        if not isinstance(center, (list, tuple)):
            if isinstance(center, str):
                parts = [p.strip() for p in center.split(',')]
                center = [float(p) for p in parts]
        rids = tree.knn(center, int(k))
        return [self.fetch_by_rid(rid) for rid in rids]

    def delete(self, column: str, key: Any) -> int:
        stats.inc("table.delete.calls")
        with stats.timer("table.delete.time"):
            tree = self._pick_index(column)
            if not tree:
                return 0
            rids = tree.search(key)
            deleted = 0
            for rid in rids:
                # En esta fase no compactamos data.dat; solo eliminar de índices
                deleted += 1
            tree.remove(key)
            self._save_indexes()
            return deleted

    def fetch_by_rid(self, rid: Tuple[int, int]) -> Dict[str, Any]:
        page_id, slot = rid
        rec = self.datafile.read_record(page_id, slot)
        return rec or {}

    def get_query_stats(self) -> Dict[str, Any]:
        """
        Obtener estadísticas de la última operación o acumuladas
        """
        # ← AGREGAR DEBUG
        print(f"🔍 DEBUG get_query_stats - Contadores disponibles: {stats.counters}")
        print(f"🔍 DEBUG get_query_stats - Timers disponibles: {list(stats.timers.keys())}")

        all_stats = {}

        for col_name, idx in self.indexes.items():
            idx_type = self.schema.indexes[col_name].name.lower()

            print(f"🔍 DEBUG Procesando índice: col={col_name}, type={idx_type}")

            # Métricas específicas del índice
            all_stats[col_name] = {
                "type": idx_type,
                "metrics": {
                    "operations": {
                        "search": {
                            "count": stats.get_counter(f"index.{idx_type}.search"),
                            "time_ms": round(stats.get_time_ms(f"index.{idx_type}.search.time"), 3),
                        },
                        "range": {
                            "count": stats.get_counter(f"index.{idx_type}.range"),
                            "time_ms": round(stats.get_time_ms(f"index.{idx_type}.range.time"), 3),
                        },
                        "add": {
                            "count": stats.get_counter(f"index.{idx_type}.add"),
                            "time_ms": round(stats.get_time_ms(f"index.{idx_type}.add.time"), 3),
                        },
                        "remove": {
                            "count": stats.get_counter(f"index.{idx_type}.remove"),
                            "time_ms": round(stats.get_time_ms(f"index.{idx_type}.remove.time"), 3),
                        },
                    },
                    "disk_access": {
                        "reads": stats.get_counter("disk.reads"),
                        "writes": stats.get_counter("disk.writes"),
                        "total": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
                    }
                }
            }

            print(f"🔍 DEBUG Stats para {col_name}: {all_stats[col_name]}")

        return all_stats
    def reset_stats(self):
        """Resetear métricas (útil para benchmarks aislados)"""
        stats.reset()