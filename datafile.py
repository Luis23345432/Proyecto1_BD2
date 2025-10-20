from __future__ import annotations

import os
import struct
from typing import Any, List, Tuple, Optional, Callable

from disk_manager import (
    DiskManager,
    PAGE_SIZE_DEFAULT,
    pack_record as default_pack,
    unpack_records as default_unpack,
)
from metrics import stats


class DataPage:
    HEADER_FMT = "<II"  # used_bytes, next_page_id
    HEADER_SIZE = struct.calcsize(HEADER_FMT)

    def __init__(
        self,
        page_size: int = PAGE_SIZE_DEFAULT,
        used_bytes: int = 0,
        next_page_id: int = 0xFFFFFFFF,  # 0xFFFFFFFF como -1 unsigned
        data: bytes | None = None,
        pack: Callable[[Any], bytes] = default_pack,
        unpack: Callable[[bytes], Tuple[List[Any], int]] = default_unpack,
    ) -> None:
        self.page_size = int(page_size)
        self.used_bytes = int(used_bytes)
        self.next_page_id = int(next_page_id)
        self.data = bytearray(data or b"" * (self.page_size - self.HEADER_SIZE))
        self.pack = pack
        self.unpack = unpack

        if len(self.data) < self.used_bytes:
            raise ValueError("used_bytes mayor que el tamaño actual de data")

    def free_space(self) -> int:
        return (self.page_size - self.HEADER_SIZE) - self.used_bytes

    def can_fit(self, record: Any) -> bool:
        encoded = self.pack(record)
        return len(encoded) <= self.free_space()

    def append_record(self, record: Any) -> bool:
        encoded = self.pack(record)
        if len(encoded) > self.free_space():
            return False
        # Extender data si es necesario
        needed = self.used_bytes + len(encoded)
        if needed > len(self.data):
            self.data.extend(b"\x00" * (needed - len(self.data)))
        self.data[self.used_bytes : self.used_bytes + len(encoded)] = encoded
        self.used_bytes += len(encoded)
        return True

    def iter_records(self) -> List[Any]:
        # Desempaqueta solo la porción utilizada
        buf = bytes(self.data[: self.used_bytes])
        recs, used = self.unpack(buf)
        return recs

    def pack_page(self) -> bytes:
        header = struct.pack(self.HEADER_FMT, self.used_bytes, self.next_page_id)
        body = bytes(self.data)
        # Ajustar body para que header+body == page_size
        max_body = self.page_size - self.HEADER_SIZE
        if len(body) < max_body:
            body += b"\x00" * (max_body - len(body))
        elif len(body) > max_body:
            body = body[:max_body]
        return header + body

    @classmethod
    def unpack_page(
        cls,
        buffer: bytes,
        pack: Callable[[Any], bytes] = default_pack,
        unpack: Callable[[bytes], Tuple[List[Any], int]] = default_unpack,
    ) -> "DataPage":
        if len(buffer) < cls.HEADER_SIZE:
            raise ValueError("buffer de página inválido")
        used_bytes, next_page_id = struct.unpack(cls.HEADER_FMT, buffer[: cls.HEADER_SIZE])
        data = bytearray(buffer[cls.HEADER_SIZE :])
        page = cls(
            page_size=len(buffer),
            used_bytes=used_bytes,
            next_page_id=next_page_id,
            data=data,
            pack=pack,
            unpack=unpack,
        )
        return page


class DataFile:
    def __init__(
        self,
        path: str,
        page_size: int = PAGE_SIZE_DEFAULT,
        pack: Callable[[Any], bytes] = default_pack,
        unpack: Callable[[bytes], Tuple[List[Any], int]] = default_unpack,
    ) -> None:
        self.path = os.path.abspath(path)
        self.page_size = int(page_size)
        self.pack = pack
        self.unpack = unpack

    def page_count(self) -> int:
        with stats.timer("io.page_count"):
            with DiskManager(self.path, page_size=self.page_size) as dm:
                stats.inc("io.diskmanager.opens")
                val = dm.page_count()
                stats.inc("io.page_count.calls")
                return val

    def read_page(self, page_id: int) -> DataPage:
        with stats.timer("io.read_page"):
            with DiskManager(self.path, page_size=self.page_size) as dm:
                stats.inc("io.diskmanager.opens")
                buf = dm.read_page(page_id)
                stats.inc("io.read_page.calls")
            return DataPage.unpack_page(buf, pack=self.pack, unpack=self.unpack)

    def write_page(self, page_id: int, page: DataPage) -> None:
        with stats.timer("io.write_page"):
            with DiskManager(self.path, page_size=self.page_size) as dm:
                stats.inc("io.diskmanager.opens")
                dm.write_page(page_id, page.pack_page())
                stats.inc("io.write_page.calls")
                dm.flush()
                stats.inc("io.flush.calls")

    def append_page(self, page: DataPage) -> int:
        with stats.timer("io.append_page"):
            with DiskManager(self.path, page_size=self.page_size) as dm:
                stats.inc("io.diskmanager.opens")
                pid = dm.append_page(page.pack_page())
                stats.inc("io.append_page.calls")
                dm.flush()
                stats.inc("io.flush.calls")
                return pid

    # Clustered append: intenta insertar en la última página, si no hay espacio crea una nueva
    def insert_clustered(self, record: Any) -> Tuple[int, int]:
        with stats.timer("io.insert_clustered"):
            with DiskManager(self.path, page_size=self.page_size) as dm:
                stats.inc("io.diskmanager.opens")
                pc = dm.page_count()
                if pc == 0:
                    page = DataPage(page_size=self.page_size, pack=self.pack, unpack=self.unpack)
                    ok = page.append_record(record)
                    if not ok:
                        raise ValueError("Registro demasiado grande para una página")
                    pid = dm.append_page(page.pack_page())
                    stats.inc("io.append_page.calls")
                    dm.flush()
                    stats.inc("io.flush.calls")
                    # slot = número de registros - 1
                    slot = len(page.iter_records()) - 1
                    return pid, max(slot, 0)
                last_pid = pc - 1
                buf = dm.read_page(last_pid)
                page = DataPage.unpack_page(buf, pack=self.pack, unpack=self.unpack)
                if page.append_record(record):
                    dm.write_page(last_pid, page.pack_page())
                    stats.inc("io.write_page.calls")
                    dm.flush()
                    stats.inc("io.flush.calls")
                    slot = len(page.iter_records()) - 1
                    return last_pid, max(slot, 0)
                # overflow: crear nueva página
                new_page = DataPage(page_size=self.page_size, pack=self.pack, unpack=self.unpack)
                ok = new_page.append_record(record)
                if not ok:
                    raise ValueError("Registro demasiado grande para una página")
                pid = dm.append_page(new_page.pack_page())
                stats.inc("io.append_page.calls")
                dm.flush()
                stats.inc("io.flush.calls")
                # opcional: encadenar páginas
                slot = len(new_page.iter_records()) - 1
                return pid, max(slot, 0)

    # Unclustered (placeholder): retorna un RID lógico (page_id, offset_dentro)
    def insert_unclustered(self, record: Any) -> Tuple[int, int]:
        return self.insert_clustered(record)

    def read_record(self, page_id: int, slot: int) -> Optional[Any]:
        page = self.read_page(page_id)
        recs = page.iter_records()
        if 0 <= slot < len(recs):
            return recs[slot]
        return None
