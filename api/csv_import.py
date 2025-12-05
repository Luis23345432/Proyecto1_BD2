"""Endpoints de la API para importación de datos CSV.

Permite cargar archivos CSV en tablas con procesamiento automático
de tipos de datos y construcción de índices.
"""
from __future__ import annotations

import csv
import os
import json
from typing import Dict, Any, List
from fastapi import APIRouter, HTTPException, UploadFile, File, Depends, Query

from .auth import get_current_user
from engine import DatabaseEngine
from indexes.spimi import build_spimi_blocks, merge_blocks
import shutil

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables/{table_name}", tags=["import"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    if user_id != current_user:
        raise HTTPException(
            status_code=403,
            detail="You don't have permission to access this user's data"
        )
    return current_user


def _parse_csv_value(value: Any, column_type: str) -> Any:
    """Convierte valores de CSV al tipo de dato apropiado según el esquema de la tabla."""
    if value is None:
        s = ""
    else:
        s = str(value)
    s = s.strip()
    if s.lower() in ("none", "null", "nan"):
        s = ""

    if column_type == 'ARRAY_FLOAT' or (isinstance(column_type, str) and column_type.startswith('ARRAY')):
        s_arr = s.strip('[]')
        try:
            return [float(x.strip()) for x in s_arr.split(',') if x.strip() != ""]
        except ValueError as e:
            raise ValueError(f"Cannot parse array value '{s}': {e}")

    if column_type == "INT":
        if s == "" or s.lower() in ("none", "null", "nan"):
            return "0"
        if not s.isdigit():
            import re
            digits = ''.join(re.findall(r"\d+", s))
            return digits if digits != "" else "0"
        return s
    if column_type == "FLOAT":
        if s == "" or s.lower() in ("none", "null", "nan"):
            return "0.0"
        try:
            float(s)
            return s
        except Exception:
            return "0.0"

    return s


@router.post("/load-csv")
async def load_csv(
        user_id: str,
        db_name: str,
        table_name: str,
        file: UploadFile = File(...),
        bulk: bool = Query(True, description="Use bulk insert mode (faster, recommended)"),
        current_user: str = Depends(_verify_user_access)
):
    """Carga un archivo CSV en una tabla, con soporte para inserción masiva y construcción de índices."""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")

    table = db.get_table(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        content = await file.read()
        text = content.decode("utf-8", errors="replace")
        try:
            sample = text[:10000]
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            class _DefaultDialect(csv.Dialect):
                delimiter = ','
                quotechar = '"'
                doublequote = True
                escapechar = None
                lineterminator = '\n'
                quoting = csv.QUOTE_MINIMAL
            dialect = _DefaultDialect()
        lines = text.splitlines()
        reader = csv.DictReader(lines, dialect=dialect)

        column_types = {col.name: col.col_type.name for col in table.schema.columns}

        rows: List[Dict[str, Any]] = []
        for row_num, row in enumerate(reader, start=2):
            parsed_row = {}
            try:
                for col_name, value in row.items():
                    if col_name in column_types:
                        col_type = column_types[col_name]
                        parsed_row[col_name] = _parse_csv_value(value, col_type)
                    else:
                        continue
                rows.append(parsed_row)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Error parsing row {row_num}, column '{col_name}': {str(e)}"
                )

        if not rows:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        if bulk:
            rids = table.insert_bulk(rows, rebuild_indexes=True)
            inserted = len(rids)
            try:
                ft_cols = [c for c, t in table.schema.indexes.items() if t.name.lower() in ("fulltext", "inverted")]
                if ft_cols:
                    for col in ft_cols:
                        block_dir = os.path.join(table.base_dir, f"spimi_blocks_{col}")
                        index_dir = os.path.join(table.base_dir, f"spimi_index_{col}")
                        os.makedirs(block_dir, exist_ok=True)
                        os.makedirs(index_dir, exist_ok=True)

                        try:
                            pc = table.datafile.page_count()
                        except Exception:
                            pc = 0

                        def doc_iter():
                            for pid in range(pc):
                                page = table.datafile.read_page(pid)
                                records = page.iter_records()
                                for slot, rec in enumerate(records):
                                    rid = (pid, slot)
                                    text = rec.get(col)
                                    if text is None:
                                        continue
                                    yield (text, rid)

                        total_docs = build_spimi_blocks(doc_iter(), block_dir, block_max_docs=200, do_stem=True)
                        merge_blocks(block_dir, index_dir, total_docs=total_docs)
                        canonical_index = os.path.join(table.base_dir, "spimi_index")
                        try:
                            if os.path.exists(canonical_index):
                                shutil.rmtree(canonical_index)
                            shutil.copytree(index_dir, canonical_index)
                        except Exception:
                            import traceback
                            print("⚠️ Warning: couldn't copy SPIMI index to canonical path:")
                            traceback.print_exc()
            except Exception:
                import traceback
                print("⚠️ Error building SPIMI index:")
                traceback.print_exc()
        else:
            inserted = 0
            for row in rows:
                table.insert(row)
                inserted += 1

    except csv.Error as e:
        raise HTTPException(status_code=400, detail=f"CSV parsing error: {str(e)}")
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error loading CSV: {str(e)}")

    return {
        "ok": True,
        "inserted": inserted,
        "mode": "bulk" if bulk else "incremental",
        "message": f"Successfully inserted {inserted} rows using {'bulk' if bulk else 'incremental'} mode"
    }