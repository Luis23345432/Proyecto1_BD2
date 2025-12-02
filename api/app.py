"""
Aplicación FastAPI del backend BD2.

Expone rutas para gestión de usuarios, bases de datos, tablas,
registros, consultas SQL, importación CSV, SPIMI y multimedia.
Configura CORS para desarrollo local o según variables de entorno.
"""
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
from .spimi import router as spimi_router


def create_app() -> FastAPI:
    app = FastAPI(title="Proyecto BD2 Backend", version="1.0.0")

    # Configura CORS: por defecto orígenes locales; puede ajustarse por env
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
        allow_origins=allowed_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=["*"],
    )

    # Registro de routers del sistema
    app.include_router(users_router)
    app.include_router(db_router)
    app.include_router(tables_router)
    app.include_router(records_router)
    app.include_router(sql_router)
    app.include_router(import_router)
    app.include_router(spimi_router)
    from .multimedia import router as multimedia_router
    app.include_router(multimedia_router)

    @app.get("/healthz")
    def healthz():
        return {"ok": True}

    return app


app = create_app()
