"""
Representa una base de datos en disco con tablas.

Carga y guarda metadatos, crea tablas nuevas y permite
recuperarlas por nombre; no elimina archivos físicamente
en `drop_table` (solo remueve del catálogo).
"""
from __future__ import annotations

import os
import json
from typing import Dict, List, Optional

from core.schema import TableSchema
from .table import Table


class Database:
    """Representa una base de datos en disco con catálogo de tablas."""
    def __init__(self, base_dir: str, name: str):
        self.base_dir = os.path.abspath(base_dir)
        self.name = name
        os.makedirs(self.base_dir, exist_ok=True)
        self.meta_path = os.path.join(self.base_dir, "metadata.json")
        self.tables: Dict[str, Table] = {}
        self._load_metadata()

    def _load_metadata(self):
        """Carga el catálogo desde metadata.json y reconstruye las tablas existentes."""
        if not os.path.exists(self.meta_path):
            self._save_metadata()
            return
        try:
            with open(self.meta_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
        except Exception:
            meta = {"tables": []}
        for tname in meta.get("tables", []):
            tdir = os.path.join(self.base_dir, "tables", tname)
            schema_path = os.path.join(tdir, "schema.json")
            if os.path.exists(schema_path):
                schema = TableSchema.load(schema_path)
                self.tables[tname] = Table(tdir, schema)

    def _save_metadata(self):
        """Persiste el catálogo de tablas en disco de forma atómica."""
        os.makedirs(os.path.join(self.base_dir, "tables"), exist_ok=True)
        data = {"name": self.name, "tables": list(self.tables.keys())}
        tmp = self.meta_path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, self.meta_path)

    def create_table(self, schema: TableSchema) -> Table:
        """Crea una nueva tabla, guarda su esquema y la registra en el catálogo."""
        tdir = os.path.join(self.base_dir, "tables", schema.name)
        os.makedirs(tdir, exist_ok=True)
        schema.suggest_indexes()
        schema.save(os.path.join(tdir, "schema.json"))
        table = Table(tdir, schema)
        self.tables[schema.name] = table
        self._save_metadata()
        return table

    def drop_table(self, name: str) -> bool:
        """Elimina la tabla del catálogo sin borrar archivos físicos."""
        if name not in self.tables:
            return False
        self.tables.pop(name)
        self._save_metadata()
        return True

    def get_table(self, name: str) -> Optional[Table]:
        """Obtiene una tabla por su nombre."""
        return self.tables.get(name)

    def list_tables(self) -> List[str]:
        """Retorna una lista con los nombres de todas las tablas."""
        return list(self.tables.keys())
