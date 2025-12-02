from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List, Optional
from datetime import datetime
import json


class ColumnType(Enum):
    INT = auto()
    VARCHAR = auto()
    DATE = auto()
    FLOAT = auto()
    ARRAY_FLOAT = auto()


class IndexType(Enum):
    BTREE = auto()
    ISAM = auto()
    AVL = auto()
    SEQUENTIAL = auto()
    HASH = auto()
    RTREE = auto()
    FULLTEXT = auto()


def _to_int(v: Any) -> Optional[int]:
    """Convierte un valor a INT, retorna None si está vacío"""
    if v is None or v == "":
        return None  # ← Cambiado: retornar None en lugar de lanzar excepción
    try:
        return int(float(v))  # float() primero para manejar "42.0"
    except (ValueError, TypeError):
        raise ValueError(f"No se puede convertir '{v}' a INT")


def _to_float(v: Any) -> Optional[float]:
    """Convierte un valor a FLOAT, retorna None si está vacío"""
    if v is None or v == "":
        return None  # ← Cambiado: retornar None en lugar de lanzar excepción
    if isinstance(v, str):
        v = v.replace(",", ".")
    try:
        return float(v)
    except (ValueError, TypeError):
        raise ValueError(f"No se puede convertir '{v}' a FLOAT")


def _to_date(v: Any) -> Optional[str]:
    """Convierte un valor a DATE (formato YYYY-MM-DD), retorna None si está vacío"""
    if v is None or v == "":
        return None  # ← Cambiado: retornar None en lugar de lanzar excepción
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    # admitir YYYY-MM-DD
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        raise ValueError(f"fecha inválida: {v}")


def _to_varchar(v: Any, max_len: Optional[int]) -> str:
    """Convierte un valor a VARCHAR con longitud máxima opcional"""
    s = "" if v is None else str(v)
    if max_len is not None and max_len > 0:
        return s[:max_len]
    return s


def _to_array_float(v: Any) -> List[float]:
    """Convierte un valor a lista de floats"""
    # CRÍTICO: NO llamar a str() si ya es lista
    if v is None:
        return []

    # Si ya es lista o tupla, convertir elementos a float
    if isinstance(v, (list, tuple)):
        result = [float(x) for x in v]
        print(f"DEBUG _to_array_float: recibió lista, retornando: {result}, tipo: {type(result)}")
        return result

    # Si es string, intentar parsearlo
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []

        # Caso 1: String JSON "[1.0, 2.0]"
        if s.startswith('[') and s.endswith(']'):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, (list, tuple)):
                    return [float(x) for x in parsed]
            except:
                pass

        # Caso 2: String separado por comas "1.0, 2.0"
        parts = [p.strip() for p in s.split(',')]
        return [float(p) for p in parts if p]

    # Fallback: retornar vacío
    print(f"WARNING _to_array_float: tipo inesperado {type(v)}, valor: {v}")
    return []


def convert_value(col_type: ColumnType, value: Any, *, max_len: Optional[int] = None) -> Any:
    """Convierte un valor al tipo de columna especificado"""
    if col_type == ColumnType.INT:
        return _to_int(value)
    if col_type == ColumnType.FLOAT:
        return _to_float(value)
    if col_type == ColumnType.DATE:
        return _to_date(value)
    if col_type == ColumnType.VARCHAR:
        return _to_varchar(value, max_len)
    if col_type == ColumnType.ARRAY_FLOAT:
        result = _to_array_float(value)
        print(f"DEBUG convert_value ARRAY_FLOAT: entrada={value} (tipo={type(value)}), salida={result} (tipo={type(result)})")
        return result
    return value