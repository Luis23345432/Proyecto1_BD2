from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional

from storage.database import Database
from parser.ast import SelectStmt, DeleteStmt, Condition


@dataclass
class Plan:
    type: str  # 'INDEX_SCAN' | 'FULL_TABLE_SCAN' | 'INSERT' | 'CREATE_TABLE' | 'DELETE'
    table: str
    column: Optional[str] = None
    condition: Optional[Condition] = None


class QueryPlanner:
    def __init__(self, db: Database):
        self.db = db

    def plan_select(self, stmt: SelectStmt) -> Plan:
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        cond = stmt.condition
        if cond and cond.op in ('=', 'BETWEEN') and cond.column in table.indexes:
            return Plan(type='INDEX_SCAN', table=stmt.table, column=cond.column, condition=cond)
        return Plan(type='FULL_TABLE_SCAN', table=stmt.table, condition=cond)

    def plan_delete(self, stmt: DeleteStmt) -> Plan:
        table = self.db.get_table(stmt.table)
        if table is None:
            raise ValueError("Tabla no existe")
        cond = stmt.condition
        if cond and cond.op == '=' and cond.column in table.indexes:
            return Plan(type='DELETE', table=stmt.table, column=cond.column, condition=cond)
        # fall back to scan then delete by key per match (simplified)
        return Plan(type='DELETE', table=stmt.table, column=cond.column if cond else None, condition=cond)
