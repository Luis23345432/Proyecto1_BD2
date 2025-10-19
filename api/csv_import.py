from __future__ import annotations

import csv
import os
from typing import Dict, Any
from fastapi import APIRouter, HTTPException, UploadFile, File

from engine import DatabaseEngine

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables/{table_name}", tags=["import"])


def _insert_rows_from_csv_lines(table, lines_iterable) -> int:
    """Shared CSV insertion helper used by multiple endpoints.

    Accepts an iterable of text lines (already decoded) and inserts into the given table.
    Returns number of inserted rows.
    """
    reader = csv.DictReader(lines_iterable)
    inserted = 0
    for row in reader:
        table.insert(dict(row))
        inserted += 1
    return inserted


@router.post("/load-csv")
async def load_csv(user_id: str, db_name: str, table_name: str, file: UploadFile = File(...)):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    t = db.get_table(table_name)
    if t is None:
        raise HTTPException(status_code=404, detail="Table not found")

    # Stream parse CSV via shared helper
    try:
        content = await file.read()
        lines = content.decode("utf-8").splitlines()
        inserted = _insert_rows_from_csv_lines(t, lines)
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, "inserted": inserted}
