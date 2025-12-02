from __future__ import annotations

import os
import time
from fastapi import APIRouter, HTTPException, Depends, Query

from .schemas import SQLQuery
from .auth import get_current_user
from parser import run_sql
from metrics import stats
from engine import DatabaseEngine  # ← AGREGAR para acceder a la tabla

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}", tags=["sql"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    if user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to access this user's data"
        )
    return current_user


def _get_active_indexes(root: str, user_id: str, db_name: str, sql: str) -> dict:
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
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
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

        # Caso especial: SELECT * FROM <tabla>; sin WHERE → usar escaneo paginado eficiente
        sql_norm = payload.sql.strip().rstrip(";")
        upper = sql_norm.upper()
        if upper.startswith("SELECT * FROM") and " WHERE " not in f" {upper} ":
            # Extraer nombre de tabla tras FROM
            try:
                table_name_part = upper.split("FROM", 1)[1].strip()
                table_name = table_name_part.split()[0]
            except Exception:
                table_name = ""

            if table_name:
                engine = DatabaseEngine(root)
                db = engine.get_database(user_id, db_name)
                if not db:
                    raise HTTPException(status_code=404, detail="Database not found")
                # Resolver tabla de forma case-insensitive
                t = db.get_table(table_name) or db.get_table(table_name.lower())
                if not t:
                    # Buscar por coincidencia case-insensitive en la lista de tablas
                    try:
                        candidates = db.list_tables()
                        match = next((n for n in candidates if n.lower() == table_name.lower()), None)
                        if match:
                            t = db.get_table(match)
                    except Exception:
                        t = None
                if not t:
                    raise HTTPException(status_code=404, detail="Table not found")

                # Escaneo paginado (igual que list_records)
                rows = []
                total_count = 0
                pc = t.datafile.page_count()
                skipped = 0
                for pid in range(pc):
                    stats.inc("disk.reads")
                    page = t.datafile.read_page(pid)
                    recs = page.iter_records()
                    page_len = len(recs)
                    total_count += page_len
                    if skipped + page_len <= offset:
                        skipped += page_len
                        continue
                    start_idx = max(0, offset - skipped)
                    for r in recs[start_idx:]:
                        rows.append(r)
                        if len(rows) >= limit:
                            break
                    skipped += page_len
                    if len(rows) >= limit:
                        break

                execution_time_ms = (time.perf_counter() - start_time) * 1000
                index_metrics = _build_index_metrics(_get_active_indexes(root, user_id, db_name, payload.sql))
                return {
                    "rows": rows,
                    "count": total_count,
                    "execution_time_ms": round(execution_time_ms, 2),
                    "metrics": {
                        "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
                        "disk_reads": stats.get_counter("disk.reads"),
                        "disk_writes": stats.get_counter("disk.writes"),
                        "indexes": index_metrics,
                    },
                }

        # Ejecutar la query normal
        out = run_sql(root, user_id, db_name, payload.sql)

        # Calcular tiempo de ejecución
        execution_time_ms = (time.perf_counter() - start_time) * 1000

        print(f"🔍 SQL: Query ejecutada exitosamente")
        print(f"🔍 SQL: stats.counters = {stats.counters}")

        # Construir métricas inteligentes
        index_metrics = _build_index_metrics(active_indexes)

        # Enriquecer respuesta con métricas y truncado si es necesario
        MAX_SQL_ROWS = 500  # umbral para evitar bloquear frontend cuando no se pagina
        if isinstance(out, dict):
            # Si contiene filas y excede el umbral, truncar
            if "rows" in out and isinstance(out["rows"], list):
                original_count = len(out["rows"])
                # Si el cliente envió limit/offset, respetar paginación en vez de truncar
                if offset > 0 or limit != 100:
                    out["rows"] = out["rows"][offset: offset + limit]
                    out["count"] = original_count if "count" not in out else out["count"]
                elif original_count > MAX_SQL_ROWS:
                    out["rows"] = out["rows"][:MAX_SQL_ROWS]
                    out["truncated"] = True
                    out["shown_rows"] = MAX_SQL_ROWS
                    out["original_count"] = original_count
            out["execution_time_ms"] = round(execution_time_ms, 2)
            out["metrics"] = {
                "total_disk_accesses": stats.get_counter("disk.reads") + stats.get_counter("disk.writes"),
                "disk_reads": stats.get_counter("disk.reads"),
                "disk_writes": stats.get_counter("disk.writes"),
                "indexes": index_metrics
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