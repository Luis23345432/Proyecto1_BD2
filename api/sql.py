from __future__ import annotations

import os
import time
from fastapi import APIRouter, HTTPException, Depends

from .schemas import SQLQuery
from .auth import get_current_user
from parser import run_sql
from metrics import stats
from engine import DatabaseEngine  # ← AGREGAR para acceder a la tabla

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}", tags=["sql"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    """Verificar que el usuario autenticado coincida con el user_id de la URL"""
    if user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to access this user's data"
        )
    return current_user


def _get_active_indexes(root: str, user_id: str, db_name: str, sql: str) -> dict:
    """
    Obtener los índices activos de la tabla mencionada en el SQL
    Retorna un diccionario con el nombre de columna -> tipo de índice
    """
    try:
        # Extraer nombre de tabla del SQL (simplificado)
        sql_upper = sql.upper()

        # Patrones comunes
        if "FROM" in sql_upper:
            parts = sql_upper.split("FROM")[1].strip().split()
            table_name = parts[0].strip().rstrip(";")
        elif "INTO" in sql_upper:
            parts = sql_upper.split("INTO")[1].strip().split()
            table_name = parts[0].strip().rstrip(";")
        elif "TABLE" in sql_upper and "CREATE" not in sql_upper:
            parts = sql_upper.split("TABLE")[1].strip().split()
            table_name = parts[0].strip().rstrip(";")
        else:
            return {}

        # Obtener la tabla
        engine = DatabaseEngine(root)
        db = engine.get_database(user_id, db_name)
        if not db:
            return {}

        t = db.get_table(table_name.lower())
        if not t:
            return {}

        # Retornar mapa de columna -> tipo de índice
        return {col: idx_type.name.lower() for col, idx_type in t.schema.indexes.items()}

    except Exception as e:
        print(f"⚠️ No se pudo detectar índices activos: {e}")
        return {}


def _build_index_metrics(active_indexes: dict) -> dict:
    """
    Construir métricas solo para los índices activos y solo operaciones con valores > 0
    """
    if not active_indexes:
        return {}

    index_metrics = {}

    for col_name, idx_type in active_indexes.items():
        # Recopilar todas las operaciones
        operations = {}

        # Búsqueda
        search_count = stats.get_counter(f"index.{idx_type}.search")
        search_time = stats.get_time_ms(f"index.{idx_type}.search.time")
        if search_count > 0 or search_time > 0:
            operations["search"] = {
                "count": search_count,
                "time_ms": round(search_time, 3)
            }

        # Rango
        range_count = stats.get_counter(f"index.{idx_type}.range")
        range_time = stats.get_time_ms(f"index.{idx_type}.range.time")
        if range_count > 0 or range_time > 0:
            operations["range"] = {
                "count": range_count,
                "time_ms": round(range_time, 3)
            }

        # Inserción
        add_count = stats.get_counter(f"index.{idx_type}.add")
        add_time = stats.get_time_ms(f"index.{idx_type}.add.time")
        if add_count > 0 or add_time > 0:
            operations["add"] = {
                "count": add_count,
                "time_ms": round(add_time, 3)
            }

        # Eliminación
        remove_count = stats.get_counter(f"index.{idx_type}.remove")
        remove_time = stats.get_time_ms(f"index.{idx_type}.remove.time")
        if remove_count > 0 or remove_time > 0:
            operations["remove"] = {
                "count": remove_count,
                "time_ms": round(remove_time, 3)
            }

        # Operaciones especiales de RTree
        if idx_type == "rtree":
            radius_count = stats.get_counter(f"index.{idx_type}.range_radius")
            radius_time = stats.get_time_ms(f"index.{idx_type}.range_radius.time")
            if radius_count > 0 or radius_time > 0:
                operations["range_radius"] = {
                    "count": radius_count,
                    "time_ms": round(radius_time, 3)
                }

            knn_count = stats.get_counter(f"index.{idx_type}.knn")
            knn_time = stats.get_time_ms(f"index.{idx_type}.knn.time")
            if knn_count > 0 or knn_time > 0:
                operations["knn"] = {
                    "count": knn_count,
                    "time_ms": round(knn_time, 3)
                }

        # Solo agregar el índice si tiene operaciones
        if operations:
            index_metrics[col_name] = {
                "type": idx_type,
                "operations": operations
            }

    return index_metrics


@router.post("/query")
def sql_query(
        user_id: str,
        db_name: str,
        payload: SQLQuery,
        current_user: str = Depends(_verify_user_access)
):
    root = os.path.dirname(os.path.dirname(__file__))

    # Resetear métricas y medir tiempo
    start_time = time.perf_counter()
    stats.reset()

    print(f"🔍 SQL: Ejecutando query: {payload.sql}")

    try:
        # Detectar índices activos de la tabla
        active_indexes = _get_active_indexes(root, user_id, db_name, payload.sql)
        print(f"🔍 SQL: Índices activos detectados: {active_indexes}")

        # Ejecutar la query
        out = run_sql(root, user_id, db_name, payload.sql)

        # Calcular tiempo de ejecución
        execution_time_ms = (time.perf_counter() - start_time) * 1000

        print(f"🔍 SQL: Query ejecutada exitosamente")
        print(f"🔍 SQL: stats.counters = {stats.counters}")

        # Construir métricas inteligentes
        index_metrics = _build_index_metrics(active_indexes)

        # Enriquecer respuesta con métricas
        if isinstance(out, dict):
            out["execution_time_ms"] = round(execution_time_ms, 2)
            out["metrics"] = {
                "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
                "disk_reads": stats.get_counter("disk.reads"),
                "disk_writes": stats.get_counter("disk.writes"),
                "indexes": index_metrics  # ← Solo índices usados con operaciones activas
            }
        else:
            out = {
                "result": out,
                "execution_time_ms": round(execution_time_ms, 2),
                "metrics": {
                    "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
                    "disk_reads": stats.get_counter("disk.reads"),
                    "disk_writes": stats.get_counter("disk.writes"),
                    "indexes": index_metrics
                }
            }

        print(f"🔍 SQL: Respuesta final: {out}")
        return out

    except Exception as e:
        print(f"❌ SQL ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))