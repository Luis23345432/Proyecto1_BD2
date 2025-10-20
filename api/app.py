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

    # CORS (default to local dev). You can override via env CORS_ALLOWED_ORIGINS (comma-separated)
    env_origins = os.getenv("CORS_ALLOWED_ORIGINS")
    if env_origins:
        allowed_origins = [o.strip() for o in env_origins.split(",") if o.strip()]
    else:
        allowed_origins = [
            "http://localhost:3000",
            "http://127.0.0.1:3000",
        ]

    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,     # no "*" when credentials=True
        allow_credentials=True,            # set to False if you don't use cookies/auth
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],               # or list what you actually send
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
