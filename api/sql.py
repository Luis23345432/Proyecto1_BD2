from __future__ import annotations

import os
from fastapi import APIRouter, HTTPException

from .schemas import SQLQuery
from parser import run_sql
# Note: CSV upload is handled by the dedicated /load-csv endpoint.

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}", tags=["sql"])


@router.post("/query")
def sql_query(user_id: str, db_name: str, payload: SQLQuery):
    root = os.path.dirname(os.path.dirname(__file__))
    try:
        out = run_sql(root, user_id, db_name, payload.sql)
        return out
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

