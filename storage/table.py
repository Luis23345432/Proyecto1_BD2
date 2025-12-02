from __future__ import annotations
from disk_manager import PAGE_SIZE_DEFAULT

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
    def __init__(self, base_dir: str, schema: TableSchema, page_size: int | None = None):
        if page_size is None:
            page_size = PAGE_SIZE_DEFAULT
        self.base_dir = os.path.abspath(base_dir)
        self.schema = schema
        self.page_size = int(page_size)
        os.makedirs(self.base_dir, exist_ok=True)
        self.data_path = os.path.join(self.base_dir, "data.dat")
        self.schema_path = os.path.join(self.base_dir, "schema.json")
        self.index_dir = os.path.join(self.base_dir, "indexes")
        os.makedirs(self.index_dir, exist_ok=True)

        # almacenamiento
        self.datafile = DataFile(self.data_path, page_size=self.page_size)

        # índices por columna
        self.indexes: Dict[str, Any] = {}
        self._initialize_indexes()

        # guardar schema si no existe
        if not os.path.exists(self.schema_path):
            self.schema.save(self.schema_path)

    def _initialize_indexes(self):
        for col_name, idx_type in self.schema.indexes.items():
            idx_path = os.path.join(self.index_dir, f"{col_name}.idx")
            idx_obj = None

            # Intentar cargar índice existente
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
                    print(f"✅ Índice {idx_type.name} cargado para columna '{col_name}'")
                except Exception as e:
                    print(f"⚠️ Error cargando índice {col_name}: {e}")
                    idx_obj = None

            # Crear nuevo índice si no existe
            if idx_obj is None:
                is_clustered = self.schema.get_column(col_name).primary_key
                if idx_type.name.lower() == 'btree':
                    idx_obj = BPlusTree(degree=5, is_clustered=is_clustered)
                elif idx_type.name.lower() == 'isam':
                    # ISAM con page_size para factor de bloque
                    idx_obj = ISAM(page_size=10, is_clustered=is_clustered)
                    print(f"🔨 ISAM creado para '{col_name}' (page_size=10)")
                elif idx_type.name.lower() == 'hash':
                    idx_obj = ExtHashing(is_clustered=is_clustered)
                elif idx_type.name.lower() == 'rtree':
                    idx_obj = RTreeIndex(dimensions=self._infer_dimensions_for(col_name))
                else:
                    idx_obj = AVL(is_clustered=is_clustered)

            self.indexes[col_name] = idx_obj

    def build_indexes_from_datafile(self):
        print(f"🔨 Construyendo índices desde datafile para '{self.schema.name}'...")

        # Recolectar todos los datos del datafile
        all_records: List[Tuple[Tuple[int, int], Dict[str, Any]]] = []

        try:
            pc = self.datafile.page_count()
        except Exception:
            print("⚠️ No hay páginas en el datafile")
            return

        # Leer todos los registros
        for page_id in range(pc):
            page = self.datafile.read_page(page_id)
            records = page.iter_records()
            for slot, rec_dict in enumerate(records):
                rid = (page_id, slot)
                all_records.append((rid, rec_dict))

        print(f"📊 Total de registros en datafile: {len(all_records)}")

        # Reconstruir cada índice
        for col_name, idx in self.indexes.items():
            idx_type = self.schema.indexes[col_name].name.lower()

            if idx_type == 'isam':
                # ISAM: construir con build_from_pairs (estructura estática base)
                pairs = []
                for rid, rec_dict in all_records:
                    key = rec_dict.get(col_name)
                    if key is not None:
                        pairs.append((key, rid))

                if pairs:
                    print(f"🔨 Construyendo ISAM para '{col_name}' con {len(pairs)} pares...")
                    idx.build_from_pairs(pairs)
                    stats_info = idx.get_stats()
                    print(f"✅ ISAM construido: {stats_info}")
                else:
                    print(f"⚠️ No hay datos para ISAM en '{col_name}'")

            elif idx_type == 'btree':
                # BTree: inserción incremental
                for rid, rec_dict in all_records:
                    key = rec_dict.get(col_name)
                    if key is not None:
                        idx.add(key, rid)
                print(f"✅ BTree construido para '{col_name}' con {len(all_records)} registros")

            elif idx_type == 'rtree':
                # RTree: datos espaciales
                for rid, rec_dict in all_records:
                    key = rec_dict.get(col_name)
                    if key is not None:
                        if not isinstance(key, (list, tuple)):
                            if isinstance(key, str):
                                parts = [p.strip() for p in key.split(',')]
                                key = [float(p) for p in parts]
                        idx.add(key, rid)
                print(f"✅ RTree construido para '{col_name}' con {len(all_records)} registros")

            else:
                # Otros índices (AVL, Hash)
                for rid, rec_dict in all_records:
                    key = rec_dict.get(col_name)
                    if key is not None:
                        idx.add(key, rid)
                print(f"✅ {idx_type.upper()} construido para '{col_name}'")

        for col_name, idx in self.indexes.items():
            if isinstance(idx, ISAM):
                stats_info = idx.get_stats()
                print(f"\n📊 ISAM Stats para '{col_name}':")
                print(f"   Páginas base: {stats_info['base_pages']}")
                print(f"   Registros base: {stats_info['base_records']}")
                print(f"   Keys índice: {idx.keys[:10]}")  # primeras 10
                if idx.pages:
                    print(f"   Primeros records de página 0: {idx.pages[0].records[:3]}")

        # Guardar índices en disco
        self._save_indexes()
        print("💾 Índices guardados en disco")

    def _save_indexes(self):
        for col_name, tree in self.indexes.items():
            idx_path = os.path.join(self.index_dir, f"{col_name}.idx")
            tree.save_idx(idx_path)

    def _infer_dimensions_for(self, column: str) -> int:
        try:
            col = self.schema.get_column(column)
            vtype = col.col_type
            if vtype.name == 'ARRAY_FLOAT':
                return 2
        except Exception:
            pass
        return 2

    def insert(self, values: Dict[str, Any]) -> Tuple[int, int]:
        stats.inc("table.insert.calls")
        with stats.timer("table.insert.time"):
            # Crear registro y validar tipos
            rec = Record(self.schema, values)
            rec_dict = rec.to_dict()

            # Insertar en datafile (storage físico)
            rid = self.datafile.insert_clustered(rec_dict)

            # Actualizar todos los índices
            for col in self.schema.columns:
                if col.name in self.indexes:
                    key = rec.values[col.name]
                    if key is None:
                        continue

                    tree = self.indexes[col.name]

                    try:
                        if isinstance(tree, RTreeIndex):
                            # RTree necesita coordenadas como lista
                            if not isinstance(key, (list, tuple)):
                                if isinstance(key, str):
                                    parts = [p.strip() for p in key.split(',')]
                                    key = [float(p) for p in parts]
                            tree.add(key, rid)
                        else:
                            # BTree, ISAM, AVL, Hash
                            tree.add(key, rid)
                    except Exception as e:
                        print(f"⚠️ Error actualizando índice {col.name}: {e}")

            # Guardar índices después de cada inserción
            self._save_indexes()
            return rid

    def insert_bulk(self, values_list: List[Dict[str, Any]], rebuild_indexes: bool = True) -> List[Tuple[int, int]]:
        stats.inc("table.insert.bulk")
        with stats.timer("table.insert.bulk.time"):
            rids = []

            if rebuild_indexes:
                # MODO BULK: Desactivar índices temporalmente
                print(f"🔨 Insertando {len(values_list)} registros en modo BULK...")
                original_indexes = self.indexes
                self.indexes = {}  # Desactivar índices

                # Insertar todos los registros en datafile
                for values in values_list:
                    rec = Record(self.schema, values)
                    rec_dict = rec.to_dict()
                    rid = self.datafile.insert_clustered(rec_dict)
                    rids.append(rid)

                # Restaurar índices
                self.indexes = original_indexes

                # Reconstruir todos los índices desde datafile
                print(f"✅ {len(rids)} registros insertados en datafile")
                print(f"🔨 Reconstruyendo índices desde datafile...")
                self.build_indexes_from_datafile()

            else:
                # MODO INCREMENTAL: Insertar uno por uno (actualiza índices)
                print(f"⚠️ Insertando {len(values_list)} registros en modo INCREMENTAL (lento)...")
                for values in values_list:
                    rid = self.insert(values)  # Usa insert() normal
                    rids.append(rid)

            return rids


    def _pick_index(self, column: str) -> Optional[Any]:
        return self.indexes.get(column)

    def search(self, column: str, key: Any) -> List[Dict[str, Any]]:
        stats.inc("table.search.calls")
        with stats.timer("table.search.time"):
            # Convertir key al tipo correcto
            try:
                col = self.schema.get_column(column)
                key = convert_value(col.col_type, key)
            except Exception:
                pass

            tree = self._pick_index(column)

            if not tree:
                # Sin índice: full scan
                print(f"⚠️ No hay índice para '{column}', haciendo full scan")
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

            # Buscar usando índice
            print(f"🔍 Buscando en {type(tree).__name__} columna='{column}', key={key}")
            rids = tree.search(key)
            print(f"🔍 Índice retornó {len(rids)} RIDs")

            # Recuperar registros completos
            results = [self.fetch_by_rid(rid) for rid in rids]
            print(f"🔍 Registros recuperados: {len(results)}")
            return results

    def range_search(self, column: str, begin_key: Any, end_key: Any) -> List[Dict[str, Any]]:
        stats.inc("table.range.calls")
        with stats.timer("table.range.time"):
            # Convertir keys al tipo correcto
            try:
                col = self.schema.get_column(column)
                begin_key = convert_value(col.col_type, begin_key)
                end_key = convert_value(col.col_type, end_key)
            except Exception:
                pass

            tree = self._pick_index(column)
            if not tree:
                print(f"⚠️ No hay índice para '{column}'")
                return []

            if isinstance(tree, RTreeIndex):
                print(f"⚠️ RTree no soporta range_search tradicional")
                rids = []
            else:
                print(f"🔍 Range search en {type(tree).__name__}: [{begin_key}, {end_key}]")
                rids = tree.range_search(begin_key, end_key)
                print(f"🔍 Range retornó {len(rids)} RIDs")

            return [self.fetch_by_rid(rid) for rid in rids]

    def range_radius(self, column: str, center: Any, radius: float) -> List[Dict[str, Any]]:
        tree = self._pick_index(column)
        if not tree or not isinstance(tree, RTreeIndex):
            return []
        if not isinstance(center, (list, tuple)):
            if isinstance(center, str):
                parts = [p.strip() for p in center.split(',')]
                center = [float(p) for p in parts]
        rids = tree.range_search_radius(center, float(radius))
        return [self.fetch_by_rid(rid) for rid in rids]

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
            deleted = len(rids)
            tree.remove(key)
            self._save_indexes()
            return deleted

    def fetch_by_rid(self, rid: Tuple[int, int]) -> Dict[str, Any]:
        page_id, slot = rid
        rec = self.datafile.read_record(page_id, slot)
        return rec or {}

    def get_query_stats(self) -> Dict[str, Any]:
        all_stats = {}
        for col_name, idx in self.indexes.items():
            idx_type = self.schema.indexes[col_name].name.lower()
            all_stats[col_name] = {
                "type": idx_type,
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
                }
            }
        return all_stats

    def reset_stats(self):
        stats.reset()