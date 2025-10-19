from __future__ import annotations

import os
from typing import Generator
from fastapi import Depends

from engine import DatabaseEngine


ROOT_DIR = os.path.dirname(__file__)  # will be replaced in app factory


def get_engine(root_dir: str = None) -> DatabaseEngine:
    rd = root_dir or os.path.dirname(os.path.abspath(__file__))
    # Project root is two levels up from api/ if installed in-place
    proj_root = os.path.dirname(rd)
    return DatabaseEngine(proj_root)
