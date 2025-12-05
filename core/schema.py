"""Módulo de esquemas de tablas y columnas.

Define las estructuras de datos para representar columnas y esquemas de tablas,
con soporte para serialización/deserialización JSON y gestión de índices.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional
import json
import os

from core.types import ColumnType, IndexType


@dataclass
class Column:
    """Representa una columna de tabla con sus atributos y restricciones."""
    name: str
    col_type: ColumnType
    length: Optional[int] = None  # para VARCHAR
    nullable: bool = True
    unique: bool = False
    primary_key: bool = False
    index: Optional[IndexType] = None  # índice recomendado/asignado

    def to_dict(self) -> Dict[str, Any]:
        """Serializa la columna a un diccionario."""
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
        """Deserializa una columna desde un diccionario."""
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
    """Representa el esquema completo de una tabla con sus columnas e índices."""
    name: str
    columns: List[Column] = field(default_factory=list)
    indexes: Dict[str, IndexType] = field(default_factory=dict)

    def add_column(self, column: Column) -> None:
        """Agrega una columna al esquema, validando que no esté duplicada."""
        if any(c.name == column.name for c in self.columns):
            raise ValueError(f"Columna duplicada: {column.name}")
        self.columns.append(column)

    def suggest_indexes(self) -> None:
        """Asigna índices por defecto según el tipo de columna y sus restricciones."""
        for col in self.columns:
            if col.primary_key or col.unique:
                col.index = col.index or IndexType.BTREE
            elif col.col_type in (ColumnType.INT, ColumnType.DATE, ColumnType.FLOAT):
                col.index = col.index or IndexType.BTREE
            else:
                col.index = col.index or None
            if col.index:
                self.indexes[col.name] = col.index

    def add_index(self, column_name: str, index_type: IndexType) -> None:
        """Agrega un índice a una columna específica."""
        if not any(c.name == column_name for c in self.columns):
            raise ValueError(f"Columna no existe: {column_name}")
        self.indexes[column_name] = index_type
        for c in self.columns:
            if c.name == column_name:
                c.index = index_type
                break

    def get_column(self, name: str) -> Column:
        """Obtiene una columna por su nombre."""
        for c in self.columns:
            if c.name == name:
                return c
        raise KeyError(name)

    def to_dict(self) -> Dict[str, Any]:
        """Serializa el esquema completo a un diccionario."""
        return {
            "name": self.name,
            "columns": [c.to_dict() for c in self.columns],
            "indexes": {k: v.name for k, v in self.indexes.items()},
        }

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "TableSchema":
        """Deserializa un esquema desde un diccionario."""
        schema = TableSchema(name=d["name"])
        schema.columns = [Column.from_dict(x) for x in d.get("columns", [])]
        idx = d.get("indexes", {})
        schema.indexes = {k: IndexType[v] for k, v in idx.items()}
        return schema

    def save(self, path: str) -> None:
        """Guarda el esquema en disco de forma atómica."""
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(self.to_dict(), f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

    @classmethod
    def load(cls, path: str) -> "TableSchema":
        """Carga un esquema desde disco, convirtiendo tipos string a enums."""
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        schema = cls(name=data["name"])

        for col_data in data["columns"]:
            col_type_str = col_data["col_type"]

            if isinstance(col_type_str, str):
                col_type = ColumnType[col_type_str]
            else:
                col_type = col_type_str

            col = Column(
                name=col_data["name"],
                col_type=col_type,
                length=col_data.get("length"),
                nullable=col_data.get("nullable", True),
                unique=col_data.get("unique", False),
                primary_key=col_data.get("primary_key", False),
            )
            schema.columns.append(col)

        for col_name, idx_type_str in data.get("indexes", {}).items():
            if isinstance(idx_type_str, str):
                idx_type = IndexType[idx_type_str]
            else:
                idx_type = idx_type_str
            schema.indexes[col_name] = idx_type

        return schema
