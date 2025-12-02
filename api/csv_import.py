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


def _parse_csv_value(value: str, column_type: str) -> Any:
    value = value.strip()

    # Arrays (para RTree): "[12.07, -77.05]" o "12.07, -77.05"
    if column_type == 'ARRAY_FLOAT' or column_type.startswith('ARRAY'):
        # Remover corchetes si existen
        value = value.strip('[]')
        # Split por comas y convertir a float
        try:
            return [float(x.strip()) for x in value.split(',')]
        except ValueError as e:
            raise ValueError(f"Cannot parse array value '{value}': {e}")

    # Para otros tipos, retornar el string tal cual
    # (el sistema de tipos de la tabla lo convertirá)
    return value


@router.post("/load-csv")
async def load_csv(
        user_id: str,
        db_name: str,
        table_name: str,
        file: UploadFile = File(...),
        bulk: bool = Query(True, description="Use bulk insert mode (faster, recommended)"),
        current_user: str = Depends(_verify_user_access)
):
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")

    table = db.get_table(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")

    try:
        # Leer CSV
        content = await file.read()
        lines = content.decode("utf-8").splitlines()
        reader = csv.DictReader(lines)

        # Obtener tipos de columnas del schema
        column_types = {col.name: col.col_type.name for col in table.schema.columns}

        # Convertir a lista de diccionarios, parseando valores especiales
        rows: List[Dict[str, Any]] = []
        for row_num, row in enumerate(reader, start=2):  # start=2 porque línea 1 es header
            parsed_row = {}
            try:
                for col_name, value in row.items():
                    if col_name in column_types:
                        col_type = column_types[col_name]
                        parsed_row[col_name] = _parse_csv_value(value, col_type)
                    else:
                        parsed_row[col_name] = value
                rows.append(parsed_row)
            except Exception as e:
                raise HTTPException(
                    status_code=400,
                    detail=f"Error parsing row {row_num}, column '{col_name}': {str(e)}"
                )

        if not rows:
            raise HTTPException(status_code=400, detail="CSV file is empty")

        # Insertar usando bulk o incremental
        if bulk:
            rids = table.insert_bulk(rows, rebuild_indexes=True)
            inserted = len(rids)
            # After bulk insert + index rebuild, also build SPIMI indexes for FULLTEXT columns
            try:
                # detect fulltext columns
                ft_cols = [c for c, t in table.schema.indexes.items() if t.name.lower() in ("fulltext", "inverted")]
                if ft_cols:
                    for col in ft_cols:
                        block_dir = os.path.join(table.base_dir, f"spimi_blocks_{col}")
                        index_dir = os.path.join(table.base_dir, f"spimi_index_{col}")
                        os.makedirs(block_dir, exist_ok=True)
                        os.makedirs(index_dir, exist_ok=True)

                        # iterate records from datafile
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
                        # Also create a canonical `spimi_index` path (used by the API search endpoint)
                        canonical_index = os.path.join(table.base_dir, "spimi_index")
                        try:
                            # remove previous canonical index if exists
                            if os.path.exists(canonical_index):
                                shutil.rmtree(canonical_index)
                            shutil.copytree(index_dir, canonical_index)
                        except Exception:
                            # non-fatal: log and continue
                            import traceback
                            print("⚠️ Warning: couldn't copy SPIMI index to canonical path:")
                            traceback.print_exc()
            except Exception:
                # Don't fail the CSV upload if SPIMI build fails; log for debugging
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
        raise  # Re-raise HTTP exceptions
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Error loading CSV: {str(e)}")

    return {
        "ok": True,
        "inserted": inserted,
        "mode": "bulk" if bulk else "incremental",
        "message": f"Successfully inserted {inserted} rows using {'bulk' if bulk else 'incremental'} mode"
    }