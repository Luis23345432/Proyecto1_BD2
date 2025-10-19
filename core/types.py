"""
Tipos y utilidades del sistema de datos (Paso 10)

- ColumnType: tipos soportados
- IndexType: tipos de índices
- convert_value: conversión/validación acorde a tipo de columna
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List, Optional
from datetime import datetime


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


def _to_int(v: Any) -> int:
    if v is None or v == "":
        raise ValueError("valor INT vacío")
    return int(v)


def _to_float(v: Any) -> float:
    if v is None or v == "":
        raise ValueError("valor FLOAT vacío")
    if isinstance(v, str):
        v = v.replace(",", ".")
    return float(v)


def _to_date(v: Any) -> str:
    # retorna ISO (YYYY-MM-DD)
    if v is None or v == "":
        raise ValueError("valor DATE vacío")
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
    s = "" if v is None else str(v)
    if max_len is not None and max_len > 0:
        return s[:max_len]
    return s


def _to_array_float(v: Any) -> List[float]:
    if v is None:
        return []
    if isinstance(v, (list, tuple)):
        return [_to_float(x) for x in v]
    # si viene como string "1,2,3"
    s = str(v).strip()
    if not s:
        return []
    parts = [p.strip() for p in s.split(',')]
    return [_to_float(p) for p in parts]


def convert_value(col_type: ColumnType, value: Any, *, max_len: Optional[int] = None) -> Any:
    """Convierte y valida el valor según el tipo de la columna."""
    if col_type == ColumnType.INT:
        return _to_int(value)
    if col_type == ColumnType.FLOAT:
        return _to_float(value)
    if col_type == ColumnType.DATE:
        return _to_date(value)
    if col_type == ColumnType.VARCHAR:
        return _to_varchar(value, max_len)
    if col_type == ColumnType.ARRAY_FLOAT:
        return _to_array_float(value)
    return value
