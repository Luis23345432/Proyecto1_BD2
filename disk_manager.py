"""
Gestor de acceso a disco basado en páginas de tamaño fijo.
Proporciona operaciones de lectura/escritura a nivel de página y
tracking de operaciones de I/O para análisis de rendimiento.
"""
from __future__ import annotations

import os
import io
import json
import struct
from typing import Any, List, Tuple


PAGE_SIZE_DEFAULT = 16384

disk_reads: int = 0
disk_writes: int = 0


class DiskManager:
    """
    Administra un archivo binario organizado en páginas de tamaño fijo.
    Cada operación de lectura/escritura trabaja con páginas completas.
    """
    def __init__(self, path: str, page_size: int = PAGE_SIZE_DEFAULT) -> None:
        """Inicializa el gestor de disco y asegura que el archivo tenga tamaño múltiplo del tamaño de página."""
        self.path = os.path.abspath(path)
        self.page_size = int(page_size)

        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            with open(self.path, "wb"):
                pass

        self._f = open(self.path, "r+b", buffering=0)

        size = os.path.getsize(self.path)
        remainder = size % self.page_size
        if remainder != 0:
            padding = self.page_size - remainder
            self._f.seek(0, io.SEEK_END)
            self._f.write(b"\x00" * padding)
            self._f.flush()
            os.fsync(self._f.fileno())

    def file_size(self) -> int:
        """Retorna el tamaño total del archivo en bytes."""
        return os.path.getsize(self.path)

    def page_count(self) -> int:
        """Retorna el número de páginas en el archivo."""
        return self.file_size() // self.page_size

    def read_page(self, page_id: int) -> bytes:
        """Lee una página completa del disco y actualiza el contador de lecturas."""
        if page_id < 0 or page_id >= self.page_count():
            raise ValueError(f"page_id fuera de rango: {page_id}")
        self._f.seek(page_id * self.page_size)
        data = self._f.read(self.page_size)
        if len(data) != self.page_size:
            raise IOError("No se pudo leer una página completa del disco")
        global disk_reads
        disk_reads += 1
        return data

    def write_page(self, page_id: int, data: bytes) -> None:
        """Escribe una página completa al disco en la posición especificada."""
        if not isinstance(data, (bytes, bytearray)):
            raise TypeError("data debe ser bytes o bytearray")
        if len(data) != self.page_size:
            raise ValueError(
                f"La página debe tener exactamente {self.page_size} bytes; recibido {len(data)}"
            )
        if page_id < 0 or page_id >= self.page_count():
            raise ValueError(f"page_id fuera de rango (use append_page para añadir): {page_id}")
        self._f.seek(page_id * self.page_size)
        self._f.write(data)
        global disk_writes
        disk_writes += 1

    def append_page(self, data: bytes | None = None) -> int:
        """Añade una nueva página al final del archivo y retorna su ID."""
        if data is None:
            data = b"\x00" * self.page_size
        elif len(data) < self.page_size:
            data = data + (b"\x00" * (self.page_size - len(data)))
        elif len(data) > self.page_size:
            raise ValueError("data excede el tamaño de página")

        new_page_id = self.page_count()
        self._f.seek(0, io.SEEK_END)
        self._f.write(data)
        global disk_writes
        disk_writes += 1
        return new_page_id

    def flush(self) -> None:
        """Sincroniza el buffer con el disco físico."""
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        """Cierra el archivo después de hacer flush."""
        try:
            self.flush()
        finally:
            self._f.close()

    def __enter__(self) -> "DiskManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


def obj_to_bytes(obj: Any) -> bytes:
    """Serializa un objeto a bytes usando JSON con codificación UTF-8."""
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def bytes_to_obj(data: bytes) -> Any:
    """Deserializa bytes a objeto desde JSON UTF-8."""
    return json.loads(data.decode("utf-8"))


def pack_record(obj: Any) -> bytes:
    """Empaqueta un registro con prefijo de longitud de 4 bytes."""
    payload = obj_to_bytes(obj)
    return struct.pack("<I", len(payload)) + payload


def unpack_records(buffer: bytes) -> Tuple[List[Any], int]:
    """
    Desempaqueta múltiples registros del buffer.
    Retorna lista de objetos y el offset final leído.
    """
    records: List[Any] = []
    offset = 0
    total = len(buffer)
    while offset + 4 <= total:
        (length,) = struct.unpack_from("<I", buffer, offset)
        if length == 0:
            break
        if offset + 4 + length > total:
            break
        start = offset + 4
        end = start + length
        payload = buffer[start:end]
        records.append(bytes_to_obj(payload))
        offset = end
    return records, offset


def get_io_counters() -> Tuple[int, int]:
    """Retorna tupla (lecturas, escrituras) de las operaciones de disco realizadas."""
    return disk_reads, disk_writes


def reset_io_counters() -> None:
    """Reinicia los contadores globales de operaciones de I/O."""
    global disk_reads, disk_writes
    disk_reads = 0
    disk_writes = 0
