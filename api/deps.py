"""Dependencias y utilidades compartidas para los endpoints de la API.

Proporciona funciones helper para obtener instancias del motor de base de datos.
"""
from __future__ import annotations

import os
from typing import Generator
from fastapi import Depends

from engine import DatabaseEngine


ROOT_DIR = os.path.dirname(__file__)


def get_engine(root_dir: str = None) -> DatabaseEngine:
    """Obtiene una instancia del motor de base de datos."""
    rd = root_dir or os.path.dirname(os.path.abspath(__file__))
    proj_root = os.path.dirname(rd)
    return DatabaseEngine(proj_root)
