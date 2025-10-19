"""
Record genérico con validación/convertidores (Paso 10)
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from core.schema import TableSchema
from core.types import ColumnType, convert_value


@dataclass
class Record:
    """Representa un registro vinculado a un TableSchema.

    Internamente se guarda como dict columna->valor ya convertido.
    """

    schema: TableSchema
    values: Dict[str, Any]

    def __post_init__(self):
        self.values = self._coerce(self.values)

    def _coerce(self, values: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for col in self.schema.columns:
            name = col.name
            if name not in values:
                if not col.nullable and not col.primary_key:
                    raise ValueError(f"Columna {name} es NOT NULL")
                out[name] = None
                continue
            v = values[name]
            out[name] = convert_value(col.col_type, v, max_len=col.length)
        return out

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.values)
