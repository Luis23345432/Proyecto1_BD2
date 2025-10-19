from __future__ import annotations

import os
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException

from .schemas import TableCreate, TableOut, TableSchemaOut, TableStatsOut
from engine import DatabaseEngine
from core.schema import Column, TableSchema
from core.types import ColumnType, IndexType

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables", tags=["tables"])


@router.post("", response_model=TableOut, status_code=201)
def create_table(user_id: str, db_name: str, payload: TableCreate) -> TableOut:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    schema = TableSchema(name=payload.name)
    # If frontend provides explicit columns, build from that; else infer from indexes
    if payload.columns:
        for c in payload.columns:
            ctype = c.get("type", "VARCHAR").upper()
            coltype = ColumnType[ctype] if ctype in ColumnType.__members__ else ColumnType.VARCHAR
            schema.add_column(Column(
                c.get("name"),
                coltype,
                length=c.get("length"),
                nullable=c.get("nullable", True),
                unique=c.get("unique", False),
                primary_key=c.get("primary_key", False),
            ))
    # indexes mapping
    if not payload.indexes and not payload.columns:
        schema.add_column(Column("id", ColumnType.INT, primary_key=True, nullable=False))
        schema.add_index("id", IndexType.BTREE)
    else:
        added_cols = {c.name for c in schema.columns}
        for idx in payload.indexes:
            if idx.column not in added_cols and not payload.columns:
                if idx.column.lower() == "id" or idx.column.lower().endswith("_id"):
                    schema.add_column(Column(idx.column, ColumnType.INT, primary_key=("id" == idx.column), nullable=False))
                else:
                    schema.add_column(Column(idx.column, ColumnType.VARCHAR, length=128, nullable=True))
                added_cols.add(idx.column)
            t = idx.type.upper()
            if t == "BTREE":
                schema.add_index(idx.column, IndexType.BTREE)
            elif t == "ISAM":
                schema.add_index(idx.column, IndexType.ISAM)
            elif t == "AVL":
                schema.add_index(idx.column, IndexType.AVL)
            elif t == "HASH":
                schema.add_index(idx.column, IndexType.HASH)
            elif t == "RTREE":
                # si el frontend usa RTREE, asumimos columna ARRAY_FLOAT si no fue declarada
                if not payload.columns:
                    # agregar columna como ARRAY_FLOAT si no existe
                    if idx.column not in added_cols:
                        schema.add_column(Column(idx.column, ColumnType.ARRAY_FLOAT, nullable=True))
                        added_cols.add(idx.column)
                schema.add_index(idx.column, IndexType.RTREE)
            else:
                schema.add_index(idx.column, IndexType.BTREE)
    table = db.create_table(schema)
    return TableOut(name=table.schema.name)


@router.get("", response_model=List[TableOut])
def list_tables(user_id: str, db_name: str) -> List[TableOut]:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        return []
    return [TableOut(name=n) for n in db.list_tables()]


@router.get("/{table_name}", response_model=TableOut)
def get_table(user_id: str, db_name: str, table_name: str) -> TableOut:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    return TableOut(name=t.schema.name)


@router.get("/{table_name}/schema", response_model=TableSchemaOut)
def get_schema(user_id: str, db_name: str, table_name: str) -> TableSchemaOut:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    cols = []
    for c in t.schema.columns:
        cols.append({
            "name": c.name,
            "type": c.col_type.name,
            "length": c.length,
            "primary_key": c.primary_key,
            "nullable": c.nullable,
        })
    idxs = {k: v.name for k, v in t.schema.indexes.items()}
    return TableSchemaOut(columns=cols, indexes=idxs)


@router.get("/{table_name}/stats", response_model=TableStatsOut)
def get_stats(user_id: str, db_name: str, table_name: str) -> TableStatsOut:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    idx_stats: Dict[str, Any] = {}
    for name, idx in t.indexes.items():
        try:
            idx_stats[name] = idx.get_stats()
        except Exception:
            idx_stats[name] = {"error": "no stats"}
    return TableStatsOut(name=table_name, indexes=idx_stats)
