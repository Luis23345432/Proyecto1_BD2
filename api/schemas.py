from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


# Users
class UserCreate(BaseModel):
    user_id: str = Field(..., min_length=1)

class User(BaseModel):
    user_id: str


# Databases
class DatabaseCreate(BaseModel):
    name: str = Field(..., min_length=1)

class DatabaseOut(BaseModel):
    name: str


# Tables
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


# Records
class RecordInsert(BaseModel):
    values: Dict[str, Any]

class RecordsQuery(BaseModel):
    limit: Optional[int] = 100
    offset: Optional[int] = 0


# Spatial queries (RTree)
class SpatialRange(BaseModel):
    column: str
    center: List[float]
    radius: float

class SpatialKNN(BaseModel):
    column: str
    center: List[float]
    k: int


# SQL
class SQLQuery(BaseModel):
    sql: str


# CSV load
class CSVLoad(BaseModel):
    path: Optional[str] = None  # for local path ingestion (optional)
