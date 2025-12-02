"""Endpoints de la API para índices de texto completo SPIMI.

Permite construir y consultar índices invertidos usando el algoritmo
SPIMI (Single-Pass In-Memory Indexing) con soporte para múltiples columnas.
"""
from __future__ import annotations

import os
from typing import Any, Dict, List, Tuple, Optional
from fastapi import APIRouter, HTTPException, Depends, Query

from .auth import get_current_user
from engine import DatabaseEngine
from indexes.spimi import build_spimi_blocks, merge_blocks, search_topk

router = APIRouter(prefix="/users/{user_id}/databases/{db_name}/tables/{table_name}/spimi", tags=["spimi"])


def _verify_user_access(user_id: str, current_user: str = Depends(get_current_user)):
    if user_id != current_user:
        raise HTTPException(status_code=403, detail="You don't have permission to access this user's data")
    return current_user


@router.post("/build")
def build_index(
    user_id: str,
    db_name: str,
    table_name: str,
    column: Optional[str] = Query(None, description="Single column to index (use 'columns' for multi)"),
    columns: Optional[List[str]] = Query(None, description="List of textual columns to concatenate and index"),
    block_max_docs: int = Query(500, description="Max docs per SPIMI block"),
    current_user: str = Depends(_verify_user_access),
):
    """Construye un índice SPIMI para una o más columnas de texto."""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    table = db.get_table(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")

    table_dir = table.base_dir
    block_dir = os.path.join(table_dir, "spimi_blocks")
    index_dir = os.path.join(table_dir, "spimi_index")
    os.makedirs(block_dir, exist_ok=True)
    os.makedirs(index_dir, exist_ok=True)

    try:
        pc = table.datafile.page_count()
    except Exception:
        raise HTTPException(status_code=400, detail="No pages in datafile")

    use_columns = [c for c in (columns or []) if (c or '').strip()]
    if not use_columns and not column:
        raise HTTPException(status_code=400, detail="Provide 'column' or 'columns' to build the index")

    def _concat_text(rec: Dict[str, Any]) -> Optional[str]:
        if use_columns:
            parts: List[str] = []
            for c in use_columns:
                v = rec.get(c)
                if v is None:
                    continue
                try:
                    parts.append(str(v))
                except Exception:
                    pass
            s = " ".join(parts).strip()
            return s if s else None
        else:
            v = rec.get(column)  # type: ignore[arg-type]
            return str(v) if v is not None else None

    def doc_iter():
        for pid in range(pc):
            page = table.datafile.read_page(pid)
            records = page.iter_records()
            for slot, rec in enumerate(records):
                rid = (pid, slot)
                text = _concat_text(rec)
                if text is None:
                    continue
                yield (text, rid)

    total_docs = build_spimi_blocks(doc_iter(), block_dir, block_max_docs=int(block_max_docs), do_stem=True)
    merge_blocks(block_dir, index_dir, total_docs=total_docs)

    return {
        "ok": True,
        "message": "SPIMI index built",
        "total_documents": total_docs,
        "index_dir": os.path.relpath(index_dir)
    }


@router.get("/search")
def spimi_search(
    user_id: str,
    db_name: str,
    table_name: str,
    query: str = Query(..., description="Natural language query string"),
    k: int = Query(10, ge=1, le=100),
    current_user: str = Depends(_verify_user_access),
):
    """Busca documentos relevantes usando el índice SPIMI con ranking TF-IDF."""
    engine = DatabaseEngine(os.path.dirname(os.path.dirname(__file__)))
    db = engine.get_database(user_id, db_name)
    if db is None:
        raise HTTPException(status_code=404, detail="Database not found")
    table = db.get_table(table_name)
    if table is None:
        raise HTTPException(status_code=404, detail="Table not found")

    index_dir = os.path.join(table.base_dir, "spimi_index")
    if not os.path.exists(index_dir):
        raise HTTPException(status_code=404, detail="SPIMI index not found; build it first")

    results = search_topk(index_dir, query, k=int(k), do_stem=True)

    out: List[Dict[str, Any]] = []
    for docid, score in results:
        try:
            page_str, slot_str = docid.split("_")
            rid = (int(page_str), int(slot_str))
            record = table.fetch_by_rid(rid)
        except Exception:
            record = {}
            rid = None
        out.append({"rid": list(rid) if rid is not None else None, "score": float(score), "record": record})

    return {"ok": True, "query": query, "k": k, "results": out}
