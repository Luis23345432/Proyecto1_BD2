from __future__ import annotations

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .users import router as users_router
from .databases import router as db_router
from .tables import router as tables_router
from .records import router as records_router
from .sql import router as sql_router
from .csv_import import router as import_router


def create_app() -> FastAPI:
    app = FastAPI(title="Proyecto BD2 Backend", version="1.0.0")

    # CORS (allow all origins by default; tighten in production)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Routers
    app.include_router(users_router)
    app.include_router(db_router)
    app.include_router(tables_router)
    app.include_router(records_router)
    app.include_router(sql_router)
    app.include_router(import_router)

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app


app = create_app()
