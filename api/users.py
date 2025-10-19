"""
Gestión de usuarios con autenticación JWT
- Register: Crear cuenta con username y password
- Login: Obtener token JWT
- Me: Ver perfil del usuario actual
"""

from __future__ import annotations

import os
import json
from typing import List, Dict
from fastapi import APIRouter, HTTPException, Depends, status
from datetime import timedelta

from .schemas import UserCreate, UserLogin, User, Token
from .auth import (
    get_password_hash,
    verify_password,
    create_access_token,
    get_current_user,
    ACCESS_TOKEN_EXPIRE_MINUTES
)
from engine import DatabaseEngine

router = APIRouter(prefix="/users", tags=["users"])


# ========================================
# FUNCIONES AUXILIARES
# ========================================

def _users_root(engine: DatabaseEngine) -> str:
    """Ruta raíz de los directorios de usuarios"""
    return os.path.join(engine.root_dir, "data", "users")


def _users_db_path(engine: DatabaseEngine) -> str:
    """Ruta al archivo JSON con credenciales de usuarios"""
    return os.path.join(engine.root_dir, "data", "users.json")


def _load_users_db(engine: DatabaseEngine) -> Dict:
    """
    Cargar base de datos de usuarios desde JSON

    Estructura:
    {
        "juan": {
            "username": "juan",
            "hashed_password": "$2b$12$..."
        }
    }
    """
    path = _users_db_path(engine)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}


def _save_users_db(engine: DatabaseEngine, users_db: Dict):
    """Guardar base de datos de usuarios en JSON"""
    path = _users_db_path(engine)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(users_db, f, indent=2, ensure_ascii=False)


# ========================================
# ENDPOINTS PÚBLICOS (sin autenticación)
# ========================================

@router.post("/register", response_model=User, status_code=201)
def register(payload: UserCreate) -> User:
    """
    Registrar un nuevo usuario

    Crea:
    - Entrada en users.json con username y password hasheado
    - Directorio: data/users/{username}/databases/
    - Base de datos por defecto: data/users/{username}/databases/default/
    """
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    users_db = _load_users_db(engine)

    # Verificar si el username ya existe
    if payload.username in users_db:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Username already exists"
        )

    # Hashear contraseña y guardar usuario
    hashed_password = get_password_hash(payload.password)
    users_db[payload.username] = {
        "username": payload.username,
        "hashed_password": hashed_password
    }
    _save_users_db(engine, users_db)

    # Crear estructura de directorios para el usuario
    user_dir = os.path.join(_users_root(engine), payload.username)
    os.makedirs(os.path.join(user_dir, "databases"), exist_ok=True)

    # Crear base de datos por defecto
    default_db_dir = os.path.join(user_dir, "databases", "default")
    os.makedirs(default_db_dir, exist_ok=True)

    return User(username=payload.username)


@router.post("/login", response_model=Token)
def login(payload: UserLogin) -> Token:
    """
    Iniciar sesión y obtener token JWT

    El token expira en 30 minutos por defecto.
    Debe enviarse en headers como: Authorization: Bearer {token}
    """
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    users_db = _load_users_db(engine)

    # Verificar si el usuario existe
    user_data = users_db.get(payload.username)
    if not user_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    # Verificar contraseña
    if not verify_password(payload.password, user_data['hashed_password']):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
        )

    # Crear token JWT
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": payload.username},
        expires_delta=access_token_expires
    )

    return Token(access_token=access_token, token_type="bearer")


# ========================================
# ENDPOINTS PROTEGIDOS (requieren token)
# ========================================

@router.get("/me", response_model=User)
def get_me(current_user: str = Depends(get_current_user)) -> User:
    """
    Obtener información del usuario actual (requiere token)

    Header requerido:
        Authorization: Bearer {token}
    """
    return User(username=current_user)


# ========================================
# ENDPOINTS DE ADMINISTRACIÓN (públicos por ahora)
# ========================================

@router.get("", response_model=List[User])
def list_users() -> List[User]:
    """Listar todos los usuarios registrados"""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    users_db = _load_users_db(engine)
    return [User(username=username) for username in users_db.keys()]


@router.get("/{username}", response_model=User)
def get_user(username: str) -> User:
    """Obtener un usuario por su username"""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    users_db = _load_users_db(engine)

    if username not in users_db:
        raise HTTPException(status_code=404, detail="User not found")

    return User(username=username)