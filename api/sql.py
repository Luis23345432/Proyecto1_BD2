from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException, Depends

from .schemas import SQLQuery
from .auth import get_current_user  # ← AGREGAR
from parser import run_sql

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}", tags=["sql"])


# ← AGREGAR ESTA FUNCIÓN
def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    """Verificar que el usuario autenticado coincida con el user_id de la URL"""
    if user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to access this user's data"
        )
    return current_user


@router.post("/query")
def sql_query(
    user_id: str,
    db_name: str,
    payload: SQLQuery,
    current_user: str = Depends(_verify_user_access)  # ← AGREGAR
):
    root = os.path.dirname(os.path.dirname(__file__))
    try:
        out = run_sql(root, user_id, db_name, payload.sql)
        return out
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))