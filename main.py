"""
Sistema de gestión de archivos y metadatos para usuarios, bases de datos y tablas.
Crea la estructura de directorios y archivos JSON necesarios para el proyecto BD2.
"""
import os
import re
import sys
import json
from typing import Optional, Any, Dict
from datetime import datetime, timezone


def _now_iso() -> str:
    """Retorna el timestamp actual en formato ISO 8601."""
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _validate_name(name: object, field: str) -> str:
    """Valida que el nombre contenga solo caracteres alfanuméricos, puntos, guiones y guiones bajos."""
    s = str(name)
    if not s:
        raise ValueError(f"{field} no puede estar vacío")
    if not re.match(r'^[A-Za-z0-9._-]+$', s):
        raise ValueError(f"{field} contiene caracteres inválidos. Use solo letras, números, '.', '_' o '-'.")
    return s


def _ensure_base(base_dir: Optional[str]) -> str:
    """Asegura que exista un directorio base; si no se proporciona, usa data/users por defecto."""
    if base_dir is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        base_dir = os.path.join(script_dir, "data", "users")
    return base_dir


def _write_json(path: str, obj: Any) -> None:
    """Escribe un objeto como JSON de forma atómica usando un archivo temporal."""
    dirpath = os.path.dirname(path)
    os.makedirs(dirpath, exist_ok=True)
    temp = path + ".tmp"
    with open(temp, "w", encoding="utf-8") as f:
        json.dump(obj, f, indent=2, ensure_ascii=False)
    os.replace(temp, path)


def create_databases_for_user(user_id: object, base_dir: Optional[str] = None) -> str:
    """Crea el directorio databases para un usuario específico."""
    uid = _validate_name(user_id, "user_id")
    base_dir = _ensure_base(base_dir)
    target = os.path.join(base_dir, uid, "databases")
    os.makedirs(target, exist_ok=True)
    return os.path.abspath(target)


def create_user(user_id: object, base_dir: Optional[str] = None) -> str:
    """Crea la estructura de directorios para un usuario y su archivo de metadatos."""
    uid = _validate_name(user_id, "user_id")
    base_dir = _ensure_base(base_dir)
    user_dir = os.path.join(base_dir, uid)
    os.makedirs(user_dir, exist_ok=True)

    meta_path = os.path.join(user_dir, "user_metadata.json")
    if not os.path.exists(meta_path):
        meta = {
            "user_id": uid,
            "created_at": _now_iso(),
            "databases": []
        }
        _write_json(meta_path, meta)

    return os.path.abspath(user_dir)


def create_database(user_id: object, db_name: object, base_dir: Optional[str] = None) -> str:
    """Crea una base de datos para un usuario, incluyendo su directorio y archivo metadata.json."""
    uid = _validate_name(user_id, "user_id")
    db = _validate_name(db_name, "db_name")
    base_dir = _ensure_base(base_dir)

    db_dir = os.path.join(base_dir, uid, "databases", db)
    os.makedirs(db_dir, exist_ok=True)

    meta_path = os.path.join(db_dir, "metadata.json")
    if not os.path.exists(meta_path):
        meta = {
            "db_name": db,
            "owner": uid,
            "created_at": _now_iso(),
            "tables": {},
            "schema_version": 1
        }
        _write_json(meta_path, meta)

    user_meta = os.path.join(base_dir, uid, "user_metadata.json")
    if os.path.exists(user_meta):
        try:
            with open(user_meta, "r", encoding="utf-8") as f:
                um = json.load(f)
        except Exception:
            um = {"databases": []}
        if db not in um.get("databases", []):
            um.setdefault("databases", []).append(db)
            _write_json(user_meta, um)

    return os.path.abspath(db_dir)


def create_table(user_id: object, db_name: object, table_name: object, base_dir: Optional[str] = None) -> str:
    """
    Crea una tabla dentro de una base de datos, generando:
    - Directorio de la tabla
    - Archivo data.dat (heap file)
    - schema.json (estructura de columnas)
    - stats.json (estadísticas de filas y páginas)
    - Directorio de índices
    """
    uid = _validate_name(user_id, "user_id")
    db = _validate_name(db_name, "db_name")
    table = _validate_name(table_name, "table_name")
    base_dir = _ensure_base(base_dir)

    tables_dir = os.path.join(base_dir, uid, "databases", db, "tables")
    table_dir = os.path.join(tables_dir, table)
    os.makedirs(table_dir, exist_ok=True)

    data_path = os.path.join(table_dir, "data.dat")
    if not os.path.exists(data_path):
        with open(data_path, "ab"):
            pass

    schema_path = os.path.join(table_dir, "schema.json")
    if not os.path.exists(schema_path):
        schema = {
            "table_name": table,
            "columns": [],
            "primary_key": None,
            "created_at": _now_iso()
        }
        _write_json(schema_path, schema)

    indexes_dir = os.path.join(table_dir, "indexes")
    os.makedirs(indexes_dir, exist_ok=True)

    stats_path = os.path.join(table_dir, "stats.json")
    if not os.path.exists(stats_path):
        stats = {
            "rows": 0,
            "pages": 0,
            "created_at": _now_iso(),
            "last_modified": _now_iso()
        }
        _write_json(stats_path, stats)

    db_meta_path = os.path.join(base_dir, uid, "databases", db, "metadata.json")
    if os.path.exists(db_meta_path):
        try:
            with open(db_meta_path, "r", encoding="utf-8") as f:
                dm = json.load(f)
        except Exception:
            dm = {"tables": {}}
        if table not in dm.get("tables", {}):
            dm.setdefault("tables", {})[table] = {
                "schema_file": os.path.relpath(schema_path, os.path.dirname(db_meta_path)),
                "created_at": _now_iso()
            }
            _write_json(db_meta_path, dm)

    return os.path.abspath(table_dir)


if __name__ == "__main__":
    """
    CLI para crear usuarios, bases de datos y tablas:
    - python main.py                    -> crea usuario demo
    - python main.py user               -> crea usuario y carpeta databases
    - python main.py user db            -> crea base de datos
    - python main.py user db table      -> crea tabla completa
    """
    args = sys.argv[1:]

    try:
        if len(args) == 0:
            user = "demo_user"
            print(f"Creando usuario de prueba: {user}")
            user_path = create_user(user)
            print(user_path)
        elif len(args) == 1:
            user = args[0]
            print(f"Creando usuario y carpeta de databases para: {user}")
            user_path = create_user(user)
            dbs_path = create_databases_for_user(user)
            print(user_path)
            print(dbs_path)
        elif len(args) == 2:
            user, db = args
            print(f"Creando database '{db}' para user '{user}'")
            create_user(user)
            db_path = create_database(user, db)
            print(db_path)
        else:
            user, db, table = args[0], args[1], args[2]
            print(f"Creando tabla '{table}' en database '{db}' para user '{user}'")
            create_user(user)
            create_database(user, db)
            table_path = create_table(user, db, table)
            print(table_path)

        sys.exit(0)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(2)
