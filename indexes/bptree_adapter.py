"""Adaptador para exponer BPlusTree/IndexInterface desde 'B+Tree.py'.

Evita problemas de import por el nombre del archivo con símbolo '+'.
"""

import importlib.util
import os
from typing import Any

_here = os.path.dirname(__file__)
_path = os.path.join(_here, "B+Tree.py")

spec = importlib.util.spec_from_file_location("bptree_impl", _path)
if spec is None or spec.loader is None:
    raise ImportError("No se pudo cargar B+Tree.py")
_mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(_mod)  # type: ignore[attr-defined]

BPlusTree = _mod.BPlusTree
IndexInterface = _mod.IndexInterface

__all__ = ["BPlusTree", "IndexInterface"]
