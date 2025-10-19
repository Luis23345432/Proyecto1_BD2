from __future__ import annotations

from typing import Any, Dict

from engine import DatabaseEngine
from .parser import SQLParser
from .planner import QueryPlanner
from .executor import QueryExecutor


def run_sql(root_dir: str, user_id: str, db_name: str, sql: str) -> Dict[str, Any]:
    eng = DatabaseEngine(root_dir)
    db = eng.get_database(user_id, db_name)
    if db is None:
        db = eng.create_database(user_id, db_name)

    stmt = SQLParser(sql).parse()
    planner = QueryPlanner(db)
    exec_ = QueryExecutor(db)

    from parser.ast import SelectStmt, InsertStmt, DeleteStmt, CreateTableStmt

    if isinstance(stmt, SelectStmt):
        plan = planner.plan_select(stmt)
        return exec_.execute_select(plan, stmt)
    if isinstance(stmt, InsertStmt):
        return exec_.execute_insert(stmt)
    if isinstance(stmt, DeleteStmt):
        plan = planner.plan_delete(stmt)
        return exec_.execute_delete(plan, stmt)
    if isinstance(stmt, CreateTableStmt):
        return exec_.execute_create_table(stmt)
    raise ValueError("Unsupported statement")
