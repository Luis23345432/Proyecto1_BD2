from __future__ import annotations

from typing import Any, Dict, List, Optional

from storage.database import Database
from .planner import Plan
from .ast import SelectStmt, InsertStmt, DeleteStmt, CreateTableStmt
from core.schema import TableSchema, Column
from core.types import ColumnType, IndexType


class QueryExecutor:
    def __init__(self, db: Database):
        self.db = db

    def execute_select(self, plan: Plan, stmt: SelectStmt) -> Dict[str, Any]:
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        if plan.type == 'INDEX_SCAN' and stmt.condition:
            if stmt.condition.op == '=':
                rows = table.search(plan.column or '', stmt.condition.value)
            else:
                rows = table.range_search(plan.column or '', stmt.condition.value, stmt.condition.value2)
        else:
            # FULL_TABLE_SCAN
            rows = table.search(stmt.condition.column, stmt.condition.value) if stmt.condition and stmt.condition.op == '=' else []
            if stmt.condition and stmt.condition.op == 'BETWEEN':
                rows = table.range_search(stmt.condition.column, stmt.condition.value, stmt.condition.value2)
            if stmt.condition is None:
                # full scan when no condition
                out: List[Dict[str, Any]] = []
                pc = table.datafile.page_count()
                for pid in range(pc):
                    page = table.datafile.read_page(pid)
                    out.extend(page.iter_records())
                rows = out
        if stmt.columns != ['*']:
            # project only selected columns
            rows = [{k: r.get(k) for k in stmt.columns} for r in rows]
        return {"rows": rows, "count": len(rows)}

    def execute_insert(self, stmt: InsertStmt) -> Dict[str, Any]:
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")

        # Handle positional values
        if "__positional__" in stmt.values:
            positional = stmt.values["__positional__"]
            column_names = [col.name for col in table.schema.columns]

            if len(positional) > len(column_names):
                raise ValueError(f"Too many values: expected {len(column_names)}, got {len(positional)}")

            mapped_values = {}
            for i, value in enumerate(positional):
                mapped_values[column_names[i]] = value

            # ← AGREGAR ESTE DEBUG
            print(f"DEBUG mapped_values: {mapped_values}")
            print(f"DEBUG ubicacion type: {type(mapped_values.get('ubicacion'))}")
            print(f"DEBUG ubicacion value: {mapped_values.get('ubicacion')}")

            rid = table.insert(mapped_values)
        else:
            rid = table.insert(stmt.values)

        return {"ok": True, "rid": rid}

    def execute_delete(self, plan: Plan, stmt: DeleteStmt) -> Dict[str, Any]:
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        if stmt.condition is None:
            return {"ok": True, "deleted": 0}
        count = table.delete(stmt.condition.column, stmt.condition.value)
        return {"ok": True, "deleted": count}

    def execute_create_table(self, stmt: CreateTableStmt) -> Dict[str, Any]:
        schema = TableSchema(name=stmt.name)
        if stmt.columns:
            # Build from DDL-style cols
            for c in stmt.columns:
                ct = self._map_type(c)
                col = Column(c.name, ct, length=c.length, primary_key=c.primary_key, nullable=not c.primary_key)
                schema.add_column(col)
                if c.index_type:
                    idx_t = self._map_index_type(c.index_type)
                    schema.add_index(c.name, idx_t)
            # If no explicit indexes, still suggest defaults
            if not schema.indexes:
                schema.suggest_indexes()
        else:
            # Legacy path from earlier implementation
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
        # Auto CSV load via CREATE ... FROM FILE has been removed; reject if present
        if stmt.csv_path:
            raise ValueError("CREATE TABLE ... FROM FILE ya no está soportado. Usa el endpoint /tables/{table}/load-csv con la tabla previamente creada.")
        return {"ok": True, "table": table.schema.name, "inserted": 0}

    def _map_type(self, c: CreateTableStmt.ColumnDecl) -> ColumnType:
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
        if t == 'ARRAY_FLOAT':  # ← AGREGAR ESTA LÍNEA
            return ColumnType.ARRAY_FLOAT
        # fallback
        return ColumnType.VARCHAR

    def _map_index_type(self, name: str) -> IndexType:
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
        return IndexType.BTREE
