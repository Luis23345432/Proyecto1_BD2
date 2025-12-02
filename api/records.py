"""Endpoints de la API para gestión de registros.

Permite insertar, listar y buscar registros en tablas,
incluidas búsquedas espaciales con RTree.
"""
from __future__ import annotations

import os
import time
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, Query, Depends

from .schemas import RecordInsert, SpatialRange, SpatialKNN
from .auth import get_current_user
from engine import DatabaseEngine
from metrics import stats

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables/{table_name}/records", tags=["records"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    if user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to access this user's data"
        )
    return current_user


@router.post("", status_code=201)
def insert_record(
        user_id: str,
        db_name: str,
        table_name: str,
        payload: RecordInsert,
        current_user: str = Depends(_verify_user_access)
):
    """Inserta un nuevo registro en la tabla."""
    start_time = time.perf_counter()
    stats.reset()

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    rid = t.insert(payload.values)
    inserted_record = t.fetch_by_rid(rid)

    execution_time_ms = (time.perf_counter() - start_time) * 1000

    print(f"🔍 stats.counters = {stats.counters}")
    print(f"🔍 stats.timers = {list(stats.timers.keys())}")

    index_metrics = t.get_query_stats()

    response = {
        "ok": True,
        "rid": list(rid) if isinstance(rid, tuple) else rid,
        "record": inserted_record,
        "execution_time_ms": round(execution_time_ms, 2),
        "metrics": {
            "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
            "disk_reads": stats.get_counter("disk.reads"),
            "disk_writes": stats.get_counter("disk.writes"),
            "indexes": index_metrics
        }
    }

    return response


@router.get("")
def list_records(
        user_id: str,
        db_name: str,
        table_name: str,
        limit: int = Query(100, ge=1, le=1000),
        offset: int = Query(0, ge=0),
        current_user: str = Depends(_verify_user_access)
):
    """Lista registros de una tabla con paginación."""
    start_time = time.perf_counter()
    stats.reset()

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    out: List[Dict[str, Any]] = []
    pc = t.datafile.page_count()
    for pid in range(pc):
        stats.inc("disk.reads")
        page = t.datafile.read_page(pid)
        out.extend(page.iter_records())

    execution_time_ms = (time.perf_counter() - start_time) * 1000

    return {
        "rows": out[offset: offset + limit],
        "count": len(out),
        "execution_time_ms": round(execution_time_ms, 2),
        "metrics": {
            "page_scans": pc,
            "disk_reads": pc
        }
    }


@router.get("/search")
def search_by_column(
        user_id: str,
        db_name: str,
        table_name: str,
        column: str = Query(..., description="Columna a buscar"),
        key: str = Query(..., description="Valor a buscar"),
        current_user: str = Depends(_verify_user_access)
):
    """Búsqueda exacta por columna usando índices disponibles."""
    start_time = time.perf_counter()
    stats.reset()

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    rows = t.search(column, key)
    execution_time_ms = (time.perf_counter() - start_time) * 1000
    index_metrics = t.get_query_stats()

    return {
        "rows": rows,
        "count": len(rows),
        "execution_time_ms": round(execution_time_ms, 2),
        "metrics": {
            "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
            "disk_reads": stats.get_counter("disk.reads"),
            "disk_writes": stats.get_counter("disk.writes"),
            "indexes": index_metrics
        }
    }


@router.get("/range")
def range_search(
        user_id: str,
        db_name: str,
        table_name: str,
        column: str = Query(...),
        begin_key: str = Query(...),
        end_key: str = Query(...),
        current_user: str = Depends(_verify_user_access)
):
    """Búsqueda por rango de valores en una columna."""
    start_time = time.perf_counter()
    stats.reset()

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    rows = t.range_search(column, begin_key, end_key)
    execution_time_ms = (time.perf_counter() - start_time) * 1000
    index_metrics = t.get_query_stats()

    return {
        "rows": rows,
        "count": len(rows),
        "execution_time_ms": round(execution_time_ms, 2),
        "metrics": {
            "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
            "disk_reads": stats.get_counter("disk.reads"),
            "disk_writes": stats.get_counter("disk.writes"),
            "indexes": index_metrics
        }
    }


@router.post("/range-radius")
def spatial_range(
        user_id: str,
        db_name: str,
        table_name: str,
        payload: SpatialRange,
        current_user: str = Depends(_verify_user_access)
):
    """Búsqueda espacial por radio usando RTree."""
    start_time = time.perf_counter()
    stats.reset()

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    rows = t.range_radius(payload.column, payload.center, payload.radius)
    execution_time_ms = (time.perf_counter() - start_time) * 1000
    index_metrics = t.get_query_stats()

    return {
        "rows": rows,
        "count": len(rows),
        "execution_time_ms": round(execution_time_ms, 2),
        "metrics": {
            "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
            "disk_reads": stats.get_counter("disk.reads"),
            "disk_writes": stats.get_counter("disk.writes"),
            "indexes": index_metrics
        }
    }


@router.post("/knn")
def spatial_knn(
        user_id: str,
        db_name: str,
        table_name: str,
        payload: SpatialKNN,
        current_user: str = Depends(_verify_user_access)
):
    """Búsqueda de k-vecinos más cercanos usando RTree."""
    start_time = time.perf_counter()
    stats.reset()

    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    rows = t.knn(payload.column, payload.center, payload.k)
    execution_time_ms = (time.perf_counter() - start_time) * 1000
    index_metrics = t.get_query_stats()

    return {
        "rows": rows,
        "count": len(rows),
        "execution_time_ms": round(execution_time_ms, 2),
        "metrics": {
            "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
            "disk_reads": stats.get_counter("disk.reads"),
            "disk_writes": stats.get_counter("disk.writes"),
            "indexes": index_metrics
        }
    }