from __future__ import annotations

import os
from typing import List
from fastapi import APIRouter, HTTPException

from .schemas import DatabaseCreate, DatabaseOut
from engine import DatabaseEngine

router = APIRouter(prefix="/users/{user_id}/databases", tags=["databases"])


@router.post("", response_model=DatabaseOut, status_code=201)
def create_database(user_id: str, payload: DatabaseCreate) -> DatabaseOut:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.create_database(user_id, payload.name)
    return DatabaseOut(name=db.name)


@router.get("", response_model=List[DatabaseOut])
def list_databases(user_id: str) -> List[DatabaseOut]:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    user_dir = os.path.join(engine.root_dir, "data", "users", user_id, "databases")
    if not os.path.isdir(user_dir):
        return []
    out = []
    for name in os.listdir(user_dir):
        if os.path.isdir(os.path.join(user_dir, name)):
            out.append(DatabaseOut(name=name))
    return out


@router.get("/{db_name}", response_model=DatabaseOut)
def get_database(user_id: str, db_name: str) -> DatabaseOut:
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    return DatabaseOut(name=db.name)


@router.delete("/{db_name}")
def delete_database(user_id: str, db_name: str):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    # just remove folder on disk
    db_dir = os.path.join(engine.root_dir, "data", "users", user_id, "databases", db_name)
    if not os.path.exists(db_dir):
        raise HTTPException(status_code=404, detail="Database not found")
    import shutil
    shutil.rmtree(db_dir, ignore_errors=True)
    return {"ok": True}
