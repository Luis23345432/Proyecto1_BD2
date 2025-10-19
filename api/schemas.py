from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# ==========================================
# AUTENTICACIÓN (NUEVO)
# ==========================================

class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)

class UserLogin(BaseModel):
    username: str
    password: str

class User(BaseModel):
    username: str

class Token(BaseModel):
    access_token: str
    token_type: str


# ==========================================
# DATABASES
# ==========================================

class DatabaseCreate(BaseModel):
    name: str = Field(..., min_length=1)

class DatabaseOut(BaseModel):
    name: str


# ==========================================
# TABLES
# ==========================================

class IndexDecl(BaseModel):
    type: str
    column: str

class TableCreate(BaseModel):
    name: str
    indexes: List[IndexDecl] = []
    columns: Optional[List[Dict[str, Any]]] = None  # optional detailed schema

class TableOut(BaseModel):
    name: str

class TableSchemaOut(BaseModel):
    columns: List[Dict[str, Any]]
    indexes: Dict[str, str]

class TableStatsOut(BaseModel):
    name: str
    indexes: Dict[str, Any]


# ==========================================
# RECORDS
# ==========================================

class RecordInsert(BaseModel):
    values: Dict[str, Any]

class RecordsQuery(BaseModel):
    limit: Optional[int] = 100
    offset: Optional[int] = 0


# ==========================================
# SPATIAL QUERIES (RTree)
# ==========================================

class SpatialRange(BaseModel):
    column: str
    center: List[float]
    radius: float

class SpatialKNN(BaseModel):
    column: str
    center: List[float]
    k: int


# ==========================================
# SQL
# ==========================================

class SQLQuery(BaseModel):
    sql: str


# ==========================================
# CSV LOAD
# ==========================================

class CSVLoad(BaseModel):
    path: Optional[str] = None  # for local path ingestion (optional)