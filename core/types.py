"""Módulo de tipos y conversiones de datos.

Define los tipos de columnas soportados (INT, VARCHAR, DATE, FLOAT, ARRAY_FLOAT),
tipos de índices disponibles y funciones de conversión/validación de valores.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, List, Optional
from datetime import datetime
import json


class ColumnType(Enum):
    """Enumeración de tipos de datos soportados para columnas."""
    INT = auto()
    VARCHAR = auto()
    DATE = auto()
    FLOAT = auto()
    ARRAY_FLOAT = auto()


class IndexType(Enum):
    """Enumeración de tipos de índices soportados para columnas."""
    BTREE = auto()
    ISAM = auto()
    AVL = auto()
    SEQUENTIAL = auto()
    HASH = auto()
    RTREE = auto()
    FULLTEXT = auto()


def _to_int(v: Any) -> int:
    """Convierte un valor a entero, validando que no sea nulo o vacío."""
    if v is None or v == "":
        raise ValueError("valor INT vacío")
    return int(v)


def _to_float(v: Any) -> float:
    """Convierte un valor a float, aceptando comas como separador decimal."""
    if v is None or v == "":
        raise ValueError("valor FLOAT vacío")
    if isinstance(v, str):
        v = v.replace(",", ".")
    return float(v)


def _to_date(v: Any) -> str:
    """Convierte un valor a fecha en formato ISO (YYYY-MM-DD)."""
    if v is None or v == "":
        raise ValueError("valor DATE vacío")
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    try:
        dt = datetime.strptime(s, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except Exception:
        raise ValueError(f"fecha inválida: {v}")


def _to_varchar(v: Any, max_len: Optional[int]) -> str:
    """Convierte un valor a cadena de texto, aplicando longitud máxima si se especifica."""
    s = "" if v is None else str(v)
    if max_len is not None and max_len > 0:
        return s[:max_len]
    return s


def _to_array_float(v: Any) -> List[float]:
    """Convierte un valor a lista de floats, aceptando listas, JSON strings o valores separados por comas."""
    if v is None:
        return []

    if isinstance(v, (list, tuple)):
        result = [float(x) for x in v]
        print(f"DEBUG _to_array_float: recibió lista, retornando: {result}, tipo: {type(result)}")
        return result

    if isinstance(v, str):
        s = v.strip()
        if not s:
            return []

        if s.startswith('[') and s.endswith(']'):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, (list, tuple)):
                    return [float(x) for x in parsed]
            except:
                pass

        parts = [p.strip() for p in s.split(',')]
        return [float(p) for p in parts if p]

    print(f"WARNING _to_array_float: tipo inesperado {type(v)}, valor: {v}")
    return []


def convert_value(col_type: ColumnType, value: Any, *, max_len: Optional[int] = None) -> Any:
    """Convierte y valida un valor según el tipo de columna especificado."""
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