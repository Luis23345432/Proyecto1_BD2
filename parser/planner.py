"""Planificador de consultas SQL.

Selecciona planes de ejecución óptimos para consultas:
- Determina si usar índices o escaneo completo.
- Detecta consultas de texto completo (SPIMI).
- Selecciona el tipo de acceso según operadores y disponibilidad de índices.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from storage.database import Database
from parser.ast import SelectStmt, DeleteStmt, Condition


@dataclass
class Plan:
    """Plan de ejecución para una consulta.
    
    Especifica el tipo de acceso (INDEX_SCAN, FULL_TABLE_SCAN, SPIMI_SEARCH, etc.)
    y los parámetros necesarios.
    """
    type: str
    table: str
    column: Optional[str] = None
    condition: Optional[Condition] = None


class QueryPlanner:
    """Planificador de consultas que selecciona planes de ejecución."""
    def __init__(self, db: Database):
        self.db = db

    def plan_select(self, stmt: SelectStmt) -> Plan:
        """Planifica la ejecución de una sentencia SELECT."""
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        cond = stmt.condition
        if cond and cond.op == '@@' and cond.column in table.indexes:
            idx_type = table.schema.indexes.get(cond.column).name.lower()
            if idx_type in ('fulltext', 'inverted'):
                return Plan(type='SPIMI_SEARCH', table=stmt.table, column=cond.column, condition=cond)
        if cond and cond.op in ('=', 'BETWEEN') and cond.column in table.indexes:
            return Plan(type='INDEX_SCAN', table=stmt.table, column=cond.column, condition=cond)
        return Plan(type='FULL_TABLE_SCAN', table=stmt.table, condition=cond)

    def plan_delete(self, stmt: DeleteStmt) -> Plan:
        """Planifica la ejecución de una sentencia DELETE."""
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        cond = stmt.condition
        if cond and cond.op == '=' and cond.column in table.indexes:
            return Plan(type='DELETE', table=stmt.table, column=cond.column, condition=cond)
        return Plan(type='DELETE', table=stmt.table, column=cond.column if cond else None, condition=cond)
