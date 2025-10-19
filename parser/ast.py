from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class Condition:
    column: str
    op: str  # '=', 'BETWEEN'
    value: Any
    value2: Any | None = None


@dataclass
class CreateTableStmt:
    name: str
    csv_path: Optional[str] = None
    indexes: List[Tuple[str, str]] = field(default_factory=list)  # (indexType, column)
    # Optional: DDL-style column declarations
    # type_name examples: INT, FLOAT, DATE, VARCHAR, ARRAY (with inner_type)
    @dataclass
    class ColumnDecl:
        name: str
        type_name: str
        length: Optional[int] = None  # for VARCHAR[n]
        is_array: bool = False
        inner_type: Optional[str] = None  # e.g., FLOAT for ARRAY[FLOAT]
        primary_key: bool = False
        index_type: Optional[str] = None  # e.g., BTREE, ISAM, RTREE, AVL, HASH

    columns: Optional[List["CreateTableStmt.ColumnDecl"]] = None


@dataclass
class SelectStmt:
    table: str
    columns: List[str]  # ['*'] for all
    condition: Optional[Condition] = None
    # Spatial extras (mutually exclusive with condition for simplicity)
    spatial: Optional[Dict[str, Any]] = None  # { kind: 'NEAR'|'KNN', column: str, center: [lat,lon], radius?: float, k?: int }


@dataclass
class InsertStmt:
    table: str
    values: Dict[str, Any]


@dataclass
class DeleteStmt:
    table: str
    condition: Optional[Condition] = None
