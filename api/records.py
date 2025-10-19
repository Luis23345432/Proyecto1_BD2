from __future__ import annotations

import os
from typing import Any, Dict, List
from fastapi import APIRouter, HTTPException, Query, Depends

from .schemas import RecordInsert, SpatialRange, SpatialKNN
from .auth import get_current_user  # ← AGREGAR
from engine import DatabaseEngine

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables/{table_name}/records", tags=["records"])


# ← AGREGAR ESTA FUNCIÓN
def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    """Verificar que el usuario autenticado coincida con el user_id de la URL"""
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
    current_user: str = Depends(_verify_user_access)  # ← AGREGAR
):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    rid = t.insert(payload.values)
    return {"ok": True, "rid": rid}


@router.get("")
def list_records(
    user_id: str,
    db_name: str,
    table_name: str,
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    current_user: str = Depends(_verify_user_access)  # ← AGREGAR
):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    # full scan (simple)
    out: List[Dict[str, Any]] = []
    pc = t.datafile.page_count()
    for pid in range(pc):
        page = t.datafile.read_page(pid)
        out.extend(page.iter_records())
    return {"rows": out[offset: offset + limit], "count": len(out)}


@router.post("/range-radius")
def spatial_range(
    user_id: str,
    db_name: str,
    table_name: str,
    payload: SpatialRange,
    current_user: str = Depends(_verify_user_access)  # ← AGREGAR
):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    rows = t.range_radius(payload.column, payload.center, payload.radius)
    return {"rows": rows, "count": len(rows)}


@router.post("/knn")
def spatial_knn(
    user_id: str,
    db_name: str,
    table_name: str,
    payload: SpatialKNN,
    current_user: str = Depends(_verify_user_access)  # ← AGREGAR
):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")
    rows = t.knn(payload.column, payload.center, payload.k)
    return {"rows": rows, "count": len(rows)}