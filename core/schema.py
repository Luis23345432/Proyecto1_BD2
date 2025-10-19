"""
Esquema de tablas y columnas (Pasos 11 y 12, parte del esquema)

- Column: definición de columna (nombre, tipo, tamaño, constraints)
- TableSchema: conjunto de columnas, claves e índices sugeridos
"""

from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
import json
import os

from core.types import ColumnType, IndexType


@dataclass
class Column:
    name: str
    col_type: ColumnType
    length: Optional[int] = None  # para VARCHAR
    nullable: bool = True
    unique: bool = False
    primary_key: bool = False
    index: Optional[IndexType] = None  # índice recomendado/asignado

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "col_type": self.col_type.name,
            "length": self.length,
            "nullable": self.nullable,
            "unique": self.unique,
            "primary_key": self.primary_key,
            "index": self.index.name if self.index else None,
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "Column":
        return Column(
            name=d["name"],
            col_type=ColumnType[d["col_type"]],
            length=d.get("length"),
            nullable=d.get("nullable", True),
            unique=d.get("unique", False),
            primary_key=d.get("primary_key", False),
            index=IndexType[d["index"]] if d.get("index") else None,
        )


@dataclass
class TableSchema:
    name: str
    columns: List[Column] = field(default_factory=list)
    # índices: mapa col_name -> IndexType
    indexes: Dict[str, IndexType] = field(default_factory=dict)

    def add_column(self, column: Column) -> None:
        if any(c.name == column.name for c in self.columns):
            raise ValueError(f"Columna duplicada: {column.name}")
        self.columns.append(column)

    def suggest_indexes(self) -> None:
        # asigne índ. por defecto según tipo o constraints
        for col in self.columns:
            if col.primary_key or col.unique:
                col.index = col.index or IndexType.BTREE
            elif col.col_type in (ColumnType.INT, ColumnType.DATE, ColumnType.FLOAT):
                col.index = col.index or IndexType.BTREE
            else:
                # VARCHAR default sin índice; el usuario puede agregar luego
                col.index = col.index or None
            if col.index:
                self.indexes[col.name] = col.index

    def add_index(self, column_name: str, index_type: IndexType) -> None:
        if not any(c.name == column_name for c in self.columns):
            raise ValueError(f"Columna no existe: {column_name}")
        self.indexes[column_name] = index_type
        for c in self.columns:
            if c.name == column_name:
                c.index = index_type
                break

    def get_column(self, name: str) -> Column:
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "indexes": {k: v.name for k, v in self.indexes.items()},
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TableSchema":
        schema = TableSchema(name=d["name"])
        schema.columns = [Column.from_dict(x) for x in d.get("columns", [])]
        idx = d.get("indexes", {})
        schema.indexes = {k: IndexType[v] for k, v in idx.items()}
        return schema

    def save(self, path: str) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    @staticmethod
    def load(path: str) -> "TableSchema":
        with open(path, "r", encoding="utf-8") as f:
            d = json.load(f)
        return TableSchema.from_dict(d)
