"""Endpoints de la API para gestión de tablas.

Permite crear, listar y consultar tablas dentro de bases de datos,
incluida la configuración de columnas e índices.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, Depends, status

from .schemas import TableCreate, TableOut, TableSchemaOut, TableStatsOut
from .auth import get_current_user
from engine import DatabaseEngine
from core.schema import Column, TableSchema
from core.types import ColumnType, IndexType

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables", tags=["tables"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    if user_id != current_user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to access this user's data"
        )
    return current_user


@router.post("", response_model=TableOut, status_code=201)
def create_table(
        user_id: str,
        db_name: str,
        payload: TableCreate,
        current_user: str = Depends(_verify_user_access)
) -> TableOut:
    """Crea una nueva tabla con el esquema e índices especificados."""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)

    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")

    schema = TableSchema(name=payload.name)

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

    if not payload.indexes and not payload.columns:
        schema.add_column(Column("id", ColumnType.INT, primary_key=True, nullable=False))
        schema.add_index("id", IndexType.BTREE)
    else:
        added_cols = {c.name for c in schema.columns}
        for idx in payload.indexes:
            if idx.column not in added_cols and not payload.columns:
                if idx.column.lower() == "id" or idx.column.lower().endswith("_id"):
                    schema.add_column(
                        Column(idx.column, ColumnType.INT, primary_key=("id" == idx.column), nullable=False))
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
                if not payload.columns:
                    if idx.column not in added_cols:
                        schema.add_column(Column(idx.column, ColumnType.ARRAY_FLOAT, nullable=True))
                        added_cols.add(idx.column)
                schema.add_index(idx.column, IndexType.RTREE)
            elif t in ("FULLTEXT", "INVERTED", "TEXT"):
                schema.add_index(idx.column, IndexType.FULLTEXT)
            else:
                schema.add_index(idx.column, IndexType.BTREE)

    table = db.create_table(schema)
    return TableOut(name=table.schema.name)


@router.get("", response_model=List[TableOut])
def list_tables(
        user_id: str,
        db_name: str,
        current_user: str = Depends(_verify_user_access)
) -> List[TableOut]:
    """Lista todas las tablas de una base de datos."""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)

    if db is None:
        return []

    return [TableOut(name=n) for n in db.list_tables()]


@router.get("/{table_name}", response_model=TableOut)
def get_table(
        user_id: str,
        db_name: str,
        table_name: str,
        current_user: str = Depends(_verify_user_access)
) -> TableOut:
    """Obtiene información de una tabla específica."""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)

    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")

    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    return TableOut(name=t.schema.name)


@router.get("/{table_name}/schema", response_model=TableSchemaOut)
def get_schema(
        user_id: str,
        db_name: str,
        table_name: str,
        current_user: str = Depends(_verify_user_access)
) -> TableSchemaOut:
    """Obtiene el esquema completo de una tabla (columnas e índices)."""
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
def get_stats(
        user_id: str,
        db_name: str,
        table_name: str,
        current_user: str = Depends(_verify_user_access)
) -> TableStatsOut:
    """Obtiene estadísticas de los índices de una tabla."""
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


@router.get("/{table_name}/indexes/{column_name}/stats")
async def get_index_stats(
        user_id: str,
        db_name: str,
        table_name: str,
        column_name: str,
        current_user: str = Depends(get_current_user)
):
    """Obtiene estadísticas detalladas de un índice específico."""
    if user_id != current_user:
        raise HTTPException(status_code=403, detail="Access denied")

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if not db:
        raise HTTPException(status_code=404, detail="Database not found")

    table = db.get_table(table_name)
    if not table:
        raise HTTPException(status_code=404, detail="Table not found")

    if column_name not in table.indexes:
        raise HTTPException(status_code=404, detail=f"No index found for column '{column_name}'")

    idx = table.indexes[column_name]

    stats = idx.get_stats()

    # Info adicional para ISAM
    if hasattr(idx, 'keys') and hasattr(idx, 'pages'):
        from indexes.ISAM import ISAM
        if isinstance(idx, ISAM):
            stats['index_keys_sample'] = idx.keys[:10]
            stats['page_details'] = []

            for i, page in enumerate(idx.pages[:5]):
                page_info = {
                    'page_index': i,
                    'records_count': len(page.records),
                    'is_full': page.is_full(),
                    'sample_records': page.records[:3]
                }
                stats['page_details'].append(page_info)
            stats['overflow_details'] = {}
            for page_idx, overflow_head in list(idx.overflow_chains.items())[:3]:
                chain_length = 0
                current = overflow_head
                while current:
                    chain_length += 1
                    current = current.next_overflow
                stats['overflow_details'][f'page_{page_idx}'] = {
                    'chain_length': chain_length,
                    'records_in_first': len(overflow_head.records)
                }

    return {
        "column": column_name,
        "stats": stats
    }