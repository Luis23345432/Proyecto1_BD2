"""Índice ISAM (Indexed Sequential Access Method).

Implementa un índice estático con páginas de datos ordenadas y
cadenas de overflow para inserciones dinámicas:
- Estructura de dos niveles: directorio de claves y páginas de datos.
- Búsqueda eficiente por clave o rango usando el directorio.
- Cadenas de overflow para manejar inserciones sin reconstruir el índice.
- Persistencia en JSON para almacenamiento y recuperación.
"""
from __future__ import annotations

import bisect
import json
import os
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass, field

from .bptree_adapter import IndexInterface
from metrics import stats


@dataclass
class ISAMPage:
    """Página de datos para el índice ISAM.
    
    Contiene registros ordenados y puede enlazar a páginas de overflow
    cuando se llena.
    """
    page_size: int
    records: List[Any] = field(default_factory=list)
    next_overflow: Optional[ISAMPage] = None

    def is_full(self) -> bool:
        return len(self.records) >= self.page_size

    def add_record(self, record: Any) -> bool:
        if not self.is_full():
            self.records.append(record)
            return True
        return False

    def to_dict(self) -> dict:
        return {
            'page_size': self.page_size,
            'records': self.records,
            'has_overflow': self.next_overflow is not None
        }


class ISAM(IndexInterface):
    """Índice ISAM con estructura estática y cadenas de overflow.
    
    Mantiene un directorio de claves que delimitan páginas de datos.
    Las inserciones se agregan a páginas de overflow si las páginas
    base están llenas.
    """
    def __init__(self,
                 page_size: int = 10,
                 is_clustered: bool = False,
                 idx_path: Optional[str] = None):
        self.page_size = page_size
        self.is_clustered = is_clustered
        self.idx_path = idx_path

        self.keys: List[Any] = []
        self.pages: List[ISAMPage] = []
        self.overflow_chains: Dict[int, ISAMPage] = {}

    def _find_page_index(self, key: Any) -> int:
        """Encuentra el índice de la página base para una clave dada."""
        if not self.keys:
            return 0
        stats.inc("disk.reads")
        i = bisect.bisect_right(self.keys, key)
        return max(0, i - 1) if i > 0 else 0

    def search(self, key: Any) -> List[Any]:
        """Busca todos los registros con una clave específica.
        
        Busca en la página base correspondiente y sus páginas de overflow.
        """
        stats.inc("index.isam.search")

        with stats.timer("index.isam.search.time"):
            page_idx = self._find_page_index(key)
            out: List[Any] = []

            if page_idx < len(self.pages):
                stats.inc("disk.reads")
                page = self.pages[page_idx]

                for record in page.records:
                    extracted_key = self._extract_key(record)
                    if extracted_key == key:
                        if isinstance(record, tuple) and len(record) == 2:
                            out.append(record[1])
                        else:
                            out.append(record)

                current_overflow = self.overflow_chains.get(page_idx)
                    for record in current_overflow.records:
                        extracted_key = self._extract_key(record)
                        if extracted_key == key:
                            if isinstance(record, tuple) and len(record) == 2:
                                out.append(record[1])
                            else:
                                out.append(record)
                    current_overflow = current_overflow.next_overflow

            return out

    def range_search(self, begin_key: Any, end_key: Any) -> List[Any]:
        """Busca todos los registros en un rango de claves [begin_key, end_key]."""
        stats.inc("index.isam.range")

        with stats.timer("index.isam.range.time"):
            start_page_idx = self._find_page_index(begin_key)
            out: List[Any] = []

            page_idx = start_page_idx
            while page_idx < len(self.pages):
                stats.inc("disk.reads")
                page = self.pages[page_idx]

                if page_idx < len(self.keys) and self.keys[page_idx] > end_key:
                    break

                for record in page.records:
                    k = self._extract_key(record)
                    if begin_key <= k <= end_key:
                        out.append(record)

                current_overflow = self.overflow_chains.get(page_idx)
                while current_overflow:
                    stats.inc("disk.reads")
                    for record in current_overflow.records:
                        k = self._extract_key(record)
                        if begin_key <= k <= end_key:
                            out.append(record)
                    current_overflow = current_overflow.next_overflow

                page_idx += 1

        return out

    def add(self, key: Any, record_or_value: Any) -> bool:
        """Agrega un registro al índice ISAM.
        
        Si la página base está llena, crea o usa cadenas de overflow.
        """
        stats.inc("index.isam.add")

        with stats.timer("index.isam.add.time"):
            record_tuple = (key, record_or_value)

            if not self.pages and not self.keys:
                self.keys.append(key)
                new_page = ISAMPage(self.page_size)
                new_page.add_record(record_tuple)
                self.pages.append(new_page)
                stats.inc("disk.writes")
                return True

            page_idx = self._find_page_index(key)

            if page_idx >= len(self.pages):
                page_idx = len(self.pages) - 1

            base_page = self.pages[page_idx]
            if not base_page.is_full():
                if base_page.add_record(record_tuple):
                    stats.inc("disk.writes")
                    return True

            if page_idx == len(self.pages) - 1 and key > self.keys[-1]:
                self.keys.append(key)
                new_page = ISAMPage(self.page_size)
                new_page.add_record(record_tuple)
                self.pages.append(new_page)
                stats.inc("disk.writes")
                return True

            if page_idx not in self.overflow_chains:
                stats.inc("disk.writes")
                self.overflow_chains[page_idx] = ISAMPage(self.page_size)
                self.overflow_chains[page_idx].add_record(record_tuple)
                return True

            current = self.overflow_chains[page_idx]
            while True:
                if current.add_record(record_tuple):
                    stats.inc("disk.writes")
                    return True

                if current.next_overflow is None:
                    stats.inc("disk.writes")
                    current.next_overflow = ISAMPage(self.page_size)
                    current.next_overflow.add_record(record_tuple)
                    return True

                current = current.next_overflow

    def remove(self, key: Any) -> bool:
        """Elimina todos los registros con una clave específica."""
        stats.inc("index.isam.remove")

        with stats.timer("index.isam.remove.time"):
            page_idx = self._find_page_index(key)
            removed = False

            if page_idx < len(self.pages):
                stats.inc("disk.reads")
                page = self.pages[page_idx]
                original_len = len(page.records)
                page.records = [r for r in page.records if self._extract_key(r) != key]
                if len(page.records) < original_len:
                    stats.inc("disk.writes")
                    removed = True

                if page_idx in self.overflow_chains:
                    current = self.overflow_chains[page_idx]
                    while current:
                        stats.inc("disk.reads")
                        original_len = len(current.records)
                        current.records = [r for r in current.records if self._extract_key(r) != key]
                        if len(current.records) < original_len:
                            stats.inc("disk.writes")
                            removed = True
                        current = current.next_overflow

        return removed

    def _extract_key(self, record: Any) -> Any:
        """Extrae la clave de un registro, manejando diferentes formatos."""
        if isinstance(record, list):
            record = tuple(record)

        if isinstance(record, tuple) and len(record) == 2:
            second = record[1]
            if isinstance(second, (tuple, list)) and len(second) == 2:
                return record[0]
            return record[0]

        if isinstance(record, dict):
            if 'key' in record:
                return record['key']

            if hasattr(self, 'key_field') and self.key_field in record:
                return record[self.key_field]

            if 'id' in record:
                return record['id']

        return record

    def build_from_pairs(self, pairs: List[Tuple[Any, Any]]):
        """Construye el índice ISAM desde una lista ordenada de pares (clave, valor)."""
        if not pairs:
            return

        pairs_sorted = sorted(pairs, key=lambda kv: kv[0])

        self.keys = []
        self.pages = []
        current_page = ISAMPage(self.page_size)

        for key, value in pairs_sorted:
            if current_page.is_full():
                self.pages.append(current_page)
                self.keys.append(key)
                current_page = ISAMPage(self.page_size)

            current_page.add_record((key, value))

        if current_page.records:
            self.pages.append(current_page)

        self.overflow_chains = {}

    def get_stats(self) -> dict:
        total_overflow_pages = 0
        total_overflow_records = 0

        for chain_head in self.overflow_chains.values():
            current = chain_head
            while current:
                total_overflow_pages += 1
                total_overflow_records += len(current.records)
                current = current.next_overflow

        total_base_records = sum(len(p.records) for p in self.pages)

        return {
            'index_type': 'ISAM',
            'clustered': self.is_clustered,
            'page_size': self.page_size,
            'base_pages': len(self.pages),
            'base_keys': len(self.keys),
            'base_records': total_base_records,
            'overflow_chains': len(self.overflow_chains),
            'overflow_pages': total_overflow_pages,
            'overflow_records': total_overflow_records,
            'total_records': total_base_records + total_overflow_records,
        }

    def save_idx(self, path: str) -> None:
        """Guarda el índice ISAM en un archivo JSON."""
        pages_data = [page.to_dict() for page in self.pages]

        overflow_data = {}
        for page_idx, chain_head in self.overflow_chains.items():
            chain = []
            current = chain_head
            while current:
                chain.append(current.to_dict())
                current = current.next_overflow
            overflow_data[str(page_idx)] = chain

        blob = {
            'meta': {
                'type': 'ISAM',
                'clustered': self.is_clustered,
                'page_size': self.page_size,
            },
            'keys': self.keys,
            'pages': pages_data,
            'overflow': overflow_data,
        }

        with open(path, 'w', encoding='utf-8') as f:
            json.dump(blob, f, ensure_ascii=False, indent=2)

    @classmethod
    def load_idx(cls, path: str) -> 'ISAM':
        """Carga el índice ISAM desde un archivo JSON."""
        with open(path, 'r', encoding='utf-8') as f:
            blob = json.load(f)

        meta = blob.get('meta', {})
        page_size = meta.get('page_size', 10)
        is_clustered = bool(meta.get('clustered', False))

        idx = cls(page_size=page_size, is_clustered=is_clustered, idx_path=path)
        idx.keys = list(blob.get('keys', []))

        pages_data = blob.get('pages', [])
        idx.pages = []
        for page_dict in pages_data:
            page = ISAMPage(page_dict['page_size'])
            page.records = [cls._list_to_tuple(rec) for rec in page_dict['records']]
            idx.pages.append(page)

        overflow_data = blob.get('overflow', {})
        idx.overflow_chains = {}
        for page_idx_str, chain_data in overflow_data.items():
            page_idx = int(page_idx_str)
            prev_page = None

            for page_dict in chain_data:
                page = ISAMPage(page_dict['page_size'])
                page.records = [cls._list_to_tuple(rec) for rec in page_dict['records']]

                if prev_page is None:
                    idx.overflow_chains[page_idx] = page
                else:
                    prev_page.next_overflow = page

                prev_page = page

        return idx

    @staticmethod
    def _list_to_tuple(obj):
        if isinstance(obj, list):
            return tuple(ISAM._list_to_tuple(item) for item in obj)
        return obj