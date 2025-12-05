"""Árbol de sintaxis abstracta (AST) para consultas SQL.

Define las estructuras de datos para representar sentencias SQL:
- CREATE TABLE: definición de tablas con columnas e índices.
- SELECT: consultas con condiciones, búsquedas espaciales y límites.
- INSERT: inserción de registros.
- DELETE: eliminación con condiciones.

Cada sentencia se representa como una dataclass con sus parámetros.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Condition:
    """Condición para cláusulas WHERE.
    
    Soporta operadores como '=', 'BETWEEN', '@@' (full-text).
    """
    column: str
    op: str
    value: Any
    value2: Any | None = None


@dataclass
class CreateTableStmt:
    """Sentencia CREATE TABLE con soporte para columnas e índices."""
    name: str
    csv_path: Optional[str] = None
    indexes: List[Tuple[str, str]] = field(default_factory=list)
    
    @dataclass
    class ColumnDecl:
        """Declaración de columna con tipo, restricciones e índices."""
        name: str
        type_name: str
        length: Optional[int] = None
        is_array: bool = False
        inner_type: Optional[str] = None
        primary_key: bool = False
        index_type: Optional[str] = None

    columns: Optional[List["CreateTableStmt.ColumnDecl"]] = None


@dataclass
class SelectStmt:
    """Sentencia SELECT con soporte para condiciones espaciales y límites."""
    table: str
    columns: List[str]
    condition: Optional[Condition] = None
    spatial: Optional[Dict[str, Any]] = None
    limit: Optional[int] = None


@dataclass
class InsertStmt:
    """Sentencia INSERT con valores posicionales o por columna."""
    table: str
    values: Dict[str, Any]


@dataclass
class DeleteStmt:
    """Sentencia DELETE con condición opcional."""
    table: str
    condition: Optional[Condition] = None
