"""
DatabaseEngine (Paso 15): Orquestador de usuarios y bases de datos.
Reutiliza el layout existente: data/users/<user>/databases/<db>
"""

from __future__ import annotations

import os
import json
from typing import Dict, Optional

from storage.database import Database
from metrics import stats


class DatabaseEngine:
    def __init__(self, root_dir: str):
        self.root_dir = os.path.abspath(root_dir)
        os.makedirs(self.root_dir, exist_ok=True)

    # paths helpers
    def _user_dir(self, user_id: str) -> str:
        return os.path.join(self.root_dir, "data", "users", user_id)

    def _db_dir(self, user_id: str, db_name: str) -> str:
        return os.path.join(self._user_dir(user_id), "databases", db_name)

    def create_database(self, user_id: str, db_name: str) -> Database:
        db_dir = self._db_dir(user_id, db_name)
        os.makedirs(os.path.join(db_dir, "tables"), exist_ok=True)
        # metadata.json mínimo
        meta_path = os.path.join(db_dir, "metadata.json")
        if not os.path.exists(meta_path):
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump({"name": db_name, "tables": []}, f)
        return Database(db_dir, db_name)

    def get_database(self, user_id: str, db_name: str) -> Optional[Database]:
        db_dir = self._db_dir(user_id, db_name)
        if not os.path.exists(db_dir):
            return None
        return Database(db_dir, db_name)

    def execute_query(self, user_id: str, db_name: str, action: str, payload: Dict) -> Dict:
        """Interfaz mínima tipo RPC para el parser futuro.

        action puede ser: create_table, insert, search, range, delete, list_tables
        """
        db = self.get_database(user_id, db_name)
        if db is None:
            raise ValueError("Database no existe")

        if action == "create_table":
            stats.inc("engine.create_table.calls")
            with stats.timer("engine.create_table.time"):
                from core.schema import TableSchema, Column
                from core.types import ColumnType

                schema = TableSchema(name=payload["name"])
                for c in payload.get("columns", []):
                    schema.add_column(Column(
                        name=c["name"],
                        col_type=ColumnType[c["type"].upper()],
                        length=c.get("length"),
                        nullable=c.get("nullable", True),
                        unique=c.get("unique", False),
                        primary_key=c.get("primary_key", False),
                    ))
                table = db.create_table(schema)
                return {"ok": True, "table": table.schema.name}

        if action == "insert":
            stats.inc("engine.insert.calls")
            with stats.timer("engine.insert.time"):
                table = db.get_table(payload["table"])  # type: ignore[index]
                if table is None:
                    raise ValueError("Tabla no existe")
                rid = table.insert(payload["values"])  # type: ignore[index]
                return {"ok": True, "rid": rid}

        if action == "search":
            stats.inc("engine.search.calls")
            with stats.timer("engine.search.time"):
                table = db.get_table(payload["table"])  # type: ignore[index]
                if table is None:
                    raise ValueError("Tabla no existe")
                rows = table.search(payload["column"], payload["key"])  # type: ignore[index]
                return {"ok": True, "rows": rows}

        if action == "range":
            stats.inc("engine.range.calls")
            with stats.timer("engine.range.time"):
                table = db.get_table(payload["table"])  # type: ignore[index]
                if table is None:
                    raise ValueError("Tabla no existe")
                rows = table.range_search(payload["column"], payload["begin"], payload["end"])  # type: ignore[index]
                return {"ok": True, "rows": rows}

        if action == "delete":
            stats.inc("engine.delete.calls")
            with stats.timer("engine.delete.time"):
                table = db.get_table(payload["table"])  # type: ignore[index]
                if table is None:
                    raise ValueError("Tabla no existe")
                count = table.delete(payload["column"], payload["key"])  # type: ignore[index]
                return {"ok": True, "deleted": count}

        if action == "list_tables":
            stats.inc("engine.list_tables.calls")
            return {"ok": True, "tables": db.list_tables()}

        raise ValueError(f"Acción no soportada: {action}")
