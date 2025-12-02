"""Esquemas Pydantic para validación de datos de la API.

Define modelos para usuarios, bases de datos, tablas, registros,
operaciones espaciales, consultas SQL y carga de CSV.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional
from pydantic import BaseModel, Field


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


class DatabaseCreate(BaseModel):
    name: str = Field(..., min_length=1)

class DatabaseOut(BaseModel):
    name: str


class IndexDecl(BaseModel):
    type: str
    column: str

class TableCreate(BaseModel):
    name: str
    indexes: List[IndexDecl] = []
    columns: Optional[List[Dict[str, Any]]] = None

class TableOut(BaseModel):
    name: str

class TableSchemaOut(BaseModel):
    columns: List[Dict[str, Any]]
    indexes: Dict[str, str]

class TableStatsOut(BaseModel):
    name: str
    indexes: Dict[str, Any]


class RecordInsert(BaseModel):
    values: Dict[str, Any]

class RecordsQuery(BaseModel):
    limit: Optional[int] = 100
    offset: Optional[int] = 0


class SpatialRange(BaseModel):
    column: str
    center: List[float]
    radius: float

class SpatialKNN(BaseModel):
    column: str
    center: List[float]
    k: int


class SQLQuery(BaseModel):
    sql: str


class CSVLoad(BaseModel):
    path: Optional[str] = None