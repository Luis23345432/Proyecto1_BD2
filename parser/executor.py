"""Ejecutor de consultas SQL.

Implementa la ejecución de sentencias SQL según los planes generados:
- SELECT: con soporte para índices, texto completo (SPIMI) y búsquedas espaciales.
- INSERT: mapea valores a columnas y valida restricciones.
- DELETE: elimina registros según condiciones.
- CREATE TABLE: crea tablas con esquemas y carga opcional de CSV.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
import os
from indexes.spimi import search_topk

from storage.database import Database
from .planner import Plan
from .ast import SelectStmt, InsertStmt, DeleteStmt, CreateTableStmt
from core.schema import TableSchema, Column
from core.types import ColumnType, IndexType


class QueryExecutor:
    """Ejecutor que implementa las operaciones SQL según los planes."""
    def __init__(self, db: Database):
        self.db = db

    def execute_select(self, plan: Plan, stmt: SelectStmt) -> Dict[str, Any]:
        """Ejecuta una consulta SELECT según el plan."""
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        if getattr(stmt, 'spatial', None):
            sp = stmt.spatial or {}
            kind = sp.get('kind')
            column = sp.get('column')
            center = sp.get('center')
            if kind == 'NEAR':
                radius = float(sp.get('radius'))
                rows = table.range_radius(column, center, radius)
            elif kind == 'KNN':
                k = int(sp.get('k', 1))
                rows = table.knn(column, center, k)
            else:
                rows = []
        elif plan.type == 'INDEX_SCAN' and stmt.condition:
            if stmt.condition.op == '=':
                rows = table.search(plan.column or '', stmt.condition.value)
            else:
                rows = table.range_search(plan.column or '', stmt.condition.value, stmt.condition.value2)
        elif plan.type == 'SPIMI_SEARCH' and stmt.condition:
            col = plan.column or stmt.condition.column
            q = stmt.condition.value
            k = stmt.limit or 10
            index_dir_candidates = [
                os.path.join(table.base_dir, 'spimi_index'),
                os.path.join(table.base_dir, f'spimi_index_{col}'),
                os.path.join(table.base_dir, f'spimi_index_{col.lower()}'),
            ]
            index_dir = None
            for cand in index_dir_candidates:
                if os.path.exists(cand):
                    index_dir = cand
                    break
            if index_dir is None:
                print(f"⚠️ SPIMI index for column '{col}' not found on disk; using in-memory inverted index if available")
                rows = table.search(col, q)
            else:
                print(f"🔍 SPIMI search: index_dir={index_dir}, query='{q}', k={k}")
                results = search_topk(index_dir, q, k=int(k), do_stem=True)
                print(f"🔍 SPIMI search returned {len(results)} results")
                out_rows = []
                for docid, score in results:
                    try:
                        page_str, slot_str = docid.split("_")
                        rid = (int(page_str), int(slot_str))
                        rec = table.fetch_by_rid(rid)
                        # Build a result object including score and rid
                        out_rows.append({
                            "rid": [rid[0], rid[1]],
                            "score": float(score),
                            "record": rec,
                        })
                    except Exception:
                        out_rows.append({"rid": None, "score": float(score), "record": {}})
                rows = out_rows
        else:
            rows = table.search(stmt.condition.column, stmt.condition.value) if stmt.condition and stmt.condition.op == '=' else []
            if stmt.condition and stmt.condition.op == 'BETWEEN':
                rows = table.range_search(stmt.condition.column, stmt.condition.value, stmt.condition.value2)
            if stmt.condition is None:
                out: List[Dict[str, Any]] = []
                pc = table.datafile.page_count()
                for pid in range(pc):
                    page = table.datafile.read_page(pid)
                    out.extend(page.iter_records())
                rows = out
        if stmt.columns == ['*']:
            flattened = []
            for r in rows:
                if isinstance(r, dict) and 'record' in r:
                    rec = r.get('record', {})
                    rec['rid'] = r.get('rid')
                    rec['score'] = r.get('score')
                    flattened.append(rec)
                else:
                    flattened.append(r)
            rows = flattened

        if stmt.columns != ['*']:
            projected_rows = []
            for r in rows:
                if isinstance(r, dict) and 'record' in r:
                    rec = r.get('record', {})
                    proj = {k: rec.get(k) for k in stmt.columns}
                    if 'rid' in r:
                        proj['rid'] = r['rid']
                    if 'score' in r:
                        proj['score'] = r['score']
                    projected_rows.append(proj)
                else:
                    projected_rows.append({k: r.get(k) for k in stmt.columns})
            rows = projected_rows
        return {"rows": rows, "count": len(rows)}

    def execute_insert(self, stmt: InsertStmt) -> Dict[str, Any]:
        """Ejecuta una inserción INSERT validando esquema."""
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")

        positional_values = stmt.values.get("__positional__", [])
        explicit_columns = stmt.values.get("__columns__", None)

        if explicit_columns:ns}")
            print(f"🔍 INSERT valores posicionales: {positional_values}")

            if len(explicit_columns) != len(positional_values):
                raise ValueError(
                    f"Column count ({len(explicit_columns)}) doesn't match "
                    f"value count ({len(positional_values)})"
                )

            mapped_values = {col: val for col, val in zip(explicit_columns, positional_values)}

            schema_column_names = {col.name for col in table.schema.columns}
            for col_name in explicit_columns:
                if col_name not in schema_column_names:
                    raise ValueError(f"Column '{col_name}' does not exist in table '{stmt.table}'")

            # Agregar valores NULL para columnas no especificadas
            for col in table.schema.columns:
                if col.name not in mapped_values:
                    # Verificar si es primary key (no puede ser NULL)
                    if col.primary_key:
                        raise ValueError(
                            f"Primary key column '{col.name}' cannot be NULL. "
                            f"Please specify a value."
                        )
                    if col.nullable:
                        mapped_values[col.name] = None
                    else:
                        raise ValueError(
                            f"Column '{col.name}' is NOT NULL but no value was provided"
                        )

            print(f"🔍 INSERT mapped_values (con columnas): {mapped_values}")

        elif "__positional__" in stmt.values:
            print(f"🔍 INSERT sin columnas explícitas (orden schema)")
            print(f"🔍 INSERT valores posicionales: {positional_values}")

            column_names = [col.name for col in table.schema.columns]

            if len(positional_values) != len(column_names):
                raise ValueError(
                    f"Expected {len(column_names)} values for columns {column_names}, "
                    f"got {len(positional_values)}"
                )

            mapped_values = {
                col_name: val
                for col_name, val in zip(column_names, positional_values)
            }

            print(f"🔍 INSERT mapped_values (sin columnas): {mapped_values}")
        else:
            print(f"🔍 INSERT valores como diccionario: {stmt.values}")
            mapped_values = stmt.values

        for key, value in mapped_values.items():
            print(f"🔍 INSERT {key}: valor={value}, tipo={type(value)}")

        rid = table.insert(mapped_values)

        return {"ok": True, "rid": list(rid) if isinstance(rid, tuple) else rid}

    def execute_delete(self, plan: Plan, stmt: DeleteStmt) -> Dict[str, Any]:
        """Ejecuta una eliminación DELETE con condiciones."""
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        if stmt.condition is None:
            return {"ok": True, "deleted": 0}
        count = table.delete(stmt.condition.column, stmt.condition.value)
        return {"ok": True, "deleted": count}

    def execute_create_table(self, stmt: CreateTableStmt) -> Dict[str, Any]:
        """Ejecuta CREATE TABLE construyendo el esquema desde DDL o configuración."""
        schema = TableSchema(name=stmt.name)
        if stmt.columns:
            for c in stmt.columns:
                ct = self._map_type(c)
                col = Column(c.name, ct, length=c.length, primary_key=c.primary_key, nullable=not c.primary_key)
                schema.add_column(col)
                if c.index_type:
                    idx_t = self._map_index_type(c.index_type)
                    schema.add_index(c.name, idx_t)
            if not schema.indexes:
                schema.suggest_indexes()
            if getattr(stmt, 'indexes', None):
                for idx_type_name, col in stmt.indexes:
                    try:
                        idx_enum = self._map_index_type(idx_type_name)
                        schema.add_index(col, idx_enum)
                    except Exception as e:
                        print(f"⚠️ Warning: couldn't add index {idx_type_name} on {col}: {e}")
        else:
            if not stmt.indexes:
                schema.add_column(Column("id", ColumnType.INT, primary_key=True, nullable=False))
            else:
                seen_pk = False
                for idx_type, col in stmt.indexes:
                    lower = col.lower()
                    if lower == 'id' or lower.endswith('_id'):
                        schema.add_column(Column(col, ColumnType.INT, primary_key=not seen_pk, nullable=False))
                        seen_pk = True
                    else:
                        if idx_type.upper() == 'RTREE':
                            schema.add_column(Column(col, ColumnType.ARRAY_FLOAT, nullable=True))
                        else:
                            schema.add_column(Column(col, ColumnType.VARCHAR, length=128, nullable=True))
                for idx_type, col in stmt.indexes:
                    tname = idx_type.upper()
                    if tname == 'BTREE':
                        schema.add_index(col, IndexType.BTREE)
                    elif tname == 'ISAM':
                        schema.add_index(col, IndexType.ISAM)
                    elif tname == 'AVL':
                        try:
                            schema.add_index(col, IndexType['AVL'])
                        except Exception:
                            schema.add_index(col, IndexType.BTREE)
                    elif tname == 'HASH':
                        schema.add_index(col, IndexType.HASH)
                    elif tname == 'RTREE':
                        schema.add_index(col, IndexType.RTREE)
            schema.suggest_indexes()
        table = self.db.create_table(schema)
        if stmt.csv_path:
            raise ValueError("CREATE TABLE ... FROM FILE ya no está soportado. Usa el endpoint /tables/{table}/load-csv con la tabla previamente creada.")
        return {"ok": True, "table": table.schema.name, "inserted": 0}

    def _map_type(self, c: CreateTableStmt.ColumnDecl) -> ColumnType:
        """Mapea un tipo de columna DDL a ColumnType enum."""
        t = c.type_name.upper()
        if t == 'INT':
            return ColumnType.INT
        if t == 'FLOAT':
            return ColumnType.FLOAT
        if t == 'DATE':
            return ColumnType.DATE
        if t == 'VARCHAR':
            return ColumnType.VARCHAR
        if t == 'ARRAY' and (c.inner_type or '').upper() == 'FLOAT':
            return ColumnType.ARRAY_FLOAT
        if t == 'ARRAY_FLOAT':
            return ColumnType.ARRAY_FLOAT
        return ColumnType.VARCHAR

    def _map_index_type(self, name: str) -> IndexType:
        """Mapea un nombre de índice a IndexType enum."""
        up = name.upper()
        if up == 'BTREE':
            return IndexType.BTREE
        if up == 'ISAM':
            return IndexType.ISAM
        if up == 'AVL':
            return IndexType.AVL if hasattr(IndexType, 'AVL') else IndexType.BTREE
        if up == 'HASH':
            return IndexType.HASH
        if up == 'RTREE':
            return IndexType.RTREE
        if up == 'FULLTEXT':
            return IndexType.FULLTEXT
        return IndexType.BTREE
