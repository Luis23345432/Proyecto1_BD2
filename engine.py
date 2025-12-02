"""
Motor de bases de datos para el proyecto BD2.
Gestiona operaciones CRUD sobre bases de datos y tablas,
registrando métricas de rendimiento para cada operación.
"""
from __future__ import annotations

import os
import json
from typing import Dict, Optional

from storage.database import Database
from metrics import stats


class DatabaseEngine:
    """
    Motor principal que coordina operaciones sobre bases de datos.
    Maneja rutas de usuarios, creación de BD y ejecución de queries.
    """
    def __init__(self, root_dir: str):
        """Inicializa el motor con un directorio raíz para almacenar todas las bases de datos."""
        self.root_dir = os.path.abspath(root_dir)
        os.makedirs(self.root_dir, exist_ok=True)

    def _user_dir(self, user_id: str) -> str:
        """Retorna el directorio del usuario."""
        return os.path.join(self.root_dir, "data", "users", user_id)

    def _db_dir(self, user_id: str, db_name: str) -> str:
        """Retorna el directorio de una base de datos específica."""
        return os.path.join(self._user_dir(user_id), "databases", db_name)

    def create_database(self, user_id: str, db_name: str) -> Database:
        """Crea una nueva base de datos con su estructura de directorios y metadatos."""
        db_dir = self._db_dir(user_id, db_name)
        os.makedirs(os.path.join(db_dir, "tables"), exist_ok=True)
        meta_path = os.path.join(db_dir, "metadata.json")
        if not os.path.exists(meta_path):
            with open(meta_path, "w", encoding="utf-8") as f:
                json.dump({"name": db_name, "tables": []}, f)
        return Database(db_dir, db_name)

    def get_database(self, user_id: str, db_name: str) -> Optional[Database]:
        """Obtiene una instancia de base de datos existente o None si no existe."""
        db_dir = self._db_dir(user_id, db_name)
        if not os.path.exists(db_dir):
            return None
        return Database(db_dir, db_name)

    def execute_query(self, user_id: str, db_name: str, action: str, payload: Dict) -> Dict:
        """
        Ejecuta una acción sobre la base de datos y retorna el resultado.
        Acciones soportadas: create_table, insert, search, range, delete, list_tables.
        Cada operación registra métricas de tiempo y número de llamadas.
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
                table = db.get_table(payload["table"])  
                if table is None:
                    raise ValueError("Tabla no existe")
                rid = table.insert(payload["values"])
                return {"ok": True, "rid": rid}

        if action == "search":
            stats.inc("engine.search.calls")
            with stats.timer("engine.search.time"):
                table = db.get_table(payload["table"])
                if table is None:
                    raise ValueError("Tabla no existe")
                rows = table.search(payload["column"], payload["key"])
                return {"ok": True, "rows": rows}

        if action == "range":
            stats.inc("engine.range.calls")
            with stats.timer("engine.range.time"):
                table = db.get_table(payload["table"])
                if table is None:
                    raise ValueError("Tabla no existe")
                rows = table.range_search(payload["column"], payload["begin"], payload["end"])
                return {"ok": True, "rows": rows}

        if action == "delete":
            stats.inc("engine.delete.calls")
            with stats.timer("engine.delete.time"):
                table = db.get_table(payload["table"])
                if table is None:
                    raise ValueError("Tabla no existe")
                count = table.delete(payload["column"], payload["key"])
                return {"ok": True, "deleted": count}

        if action == "list_tables":
            stats.inc("engine.list_tables.calls")
            return {"ok": True, "tables": db.list_tables()}

        raise ValueError(f"Acción no soportada: {action}")
