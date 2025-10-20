"""
Sistema de autenticación con JWT
- Hash de contraseñas con bcrypt
- Tokens JWT con expiración
- Dependency para proteger endpoints
"""

import os
from datetime import datetime, timedelta
from typing import Optional
from jose import JWTError, jwt
from passlib.context import CryptContext
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# ========================================
# CONFIGURACIÓN DE SEGURIDAD
# ========================================

# SECRET_KEY can be overridden via environment variable JWT_SECRET
SECRET_KEY = os.getenv(
    "JWT_SECRET",
    "d8f7a6b5c4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a9b8c7d6e5f4a3b2c1d0e9f8a7",  # default for local/dev
)
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

# Contexto para hashear contraseñas
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

# Security scheme para extraer el Bearer token
security = HTTPBearer()


# ========================================
# FUNCIONES DE HASHING
# ========================================

def verify_password(plain_password: str, hashed_password: str) -> bool:
    """Verificar si una contraseña coincide con su hash"""
    return pwd_context.verify(plain_password, hashed_password)


def get_password_hash(password: str) -> str:
    """Generar hash de una contraseña"""
    return pwd_context.hash(password)


# ========================================
# FUNCIONES JWT
# ========================================

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """
    Crear un token JWT

    Args:
        data: Diccionario con datos a codificar (típicamente {"sub": username})
        expires_delta: Tiempo de expiración (por defecto 30 minutos)

    Returns:
        Token JWT como string
    """
    to_encode = data.copy()

    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


def decode_token(token: str) -> dict:
    """
    Decodificar y validar un token JWT

    Args:
        token: Token JWT como string

    Returns:
        Payload del token como diccionario

    Raises:
        HTTPException: Si el token es inválido o expiró
    """
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ========================================
# DEPENDENCY PARA PROTEGER ENDPOINTS
# ========================================

async def get_current_user(
        credentials: HTTPAuthorizationCredentials = Depends(security)
) -> str:
    """
    Dependency para obtener el usuario actual desde el token JWT

    Uso:
        @router.get("/protected")
        def protected_route(current_user: str = Depends(get_current_user)):
            return {"user": current_user}

    Args:
        credentials: Credenciales HTTP Bearer extraídas automáticamente

    Returns:
        Username del usuario autenticado

    Raises:
        HTTPException 401: Si el token es inválido o no contiene username
    """
    token = credentials.credentials
    payload = decode_token(token)

    username: str = payload.get("sub")
    if username is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )

    return username