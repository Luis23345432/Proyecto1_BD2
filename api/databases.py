"""
Gestión de bases de datos (PROTEGIDO)
- Cada usuario solo puede acceder a sus propias bases de datos
- Se valida que user_id en la URL coincida con el usuario autenticado
"""

from __future__ import annotations

import os
from typing import List
from fastapi import APIRouter, HTTPException, Depends, status

from .schemas import DatabaseCreate, DatabaseOut
from .auth import get_current_user
from engine import DatabaseEngine

router = APIRouter(prefix="/users/{user_id}/databases", tags=["databases"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    """
    Verificar que el usuario autenticado coincida con el user_id de la URL

    Previene que un usuario acceda a las bases de datos de otro.
    """
    if user_id != current_user:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="You don't have permission to access this user's databases"
        )
    return current_user


@router.post("", response_model=DatabaseOut, status_code=201)
def create_database(
        user_id: str,
        payload: DatabaseCreate,
        current_user: str = Depends(_verify_user_access)
) -> DatabaseOut:
    """
    Crear una nueva base de datos para el usuario autenticado

    Requiere:
        - Token JWT válido
        - user_id en URL debe coincidir con el usuario autenticado
    """
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.create_database(user_id, payload.name)
    return DatabaseOut(name=db.name)


@router.get("", response_model=List[DatabaseOut])
def list_databases(
        user_id: str,
        current_user: str = Depends(_verify_user_access)
) -> List[DatabaseOut]:
    """
    Listar todas las bases de datos del usuario autenticado
    """
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
def get_database(
        user_id: str,
        db_name: str,
        current_user: str = Depends(_verify_user_access)
) -> DatabaseOut:
    """
    Obtener información de una base de datos específica
    """
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)

    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")

    return DatabaseOut(name=db.name)


@router.delete("/{db_name}")
def delete_database(
        user_id: str,
        db_name: str,
        current_user: str = Depends(_verify_user_access)
):
    """
    Eliminar una base de datos del usuario autenticado
    """
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db_dir = os.path.join(engine.root_dir, "data", "users", user_id, "databases", db_name)

    if not os.path.exists(db_dir):
        raise HTTPException(status_code=404, detail="Database not found")

    import shutil
    shutil.rmtree(db_dir, ignore_errors=True)
    return {"ok": True, "message": f"Database '{db_name}' deleted successfully"}