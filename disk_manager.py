"""
DiskManager + Serialización (Paso 2)

Este módulo provee:
- DiskManager: lectura/escritura de páginas fijas (por defecto 4096 bytes) en un archivo de datos (data.dat).
- Utilidades de serialización simple para convertir objetos Python <-> bytes mediante JSON
  y empaquetarlos con un prefijo de longitud de 4 bytes (little-endian) para flujos.

No gestiona el formato interno de una página (eso es parte del Paso 3: DataFile + DataPage).
"""

from __future__ import annotations

import os
import io
import json
import struct
from typing import Any, List, Tuple


PAGE_SIZE_DEFAULT = 4096

# Contadores de I/O (globales del módulo)
disk_reads: int = 0
disk_writes: int = 0


class DiskManager:
    """Administrador de páginas en disco para un único archivo de datos.

    Responsabilidades:
      - Abrir/crear el archivo de datos (por ejemplo, data.dat de una tabla).
      - Asegurar que el tamaño del archivo sea múltiplo del tamaño de página, rellenando con ceros si es necesario.
      - Leer una página completa (read_page) dado un page_id.
      - Escribir una página completa (write_page) en un page_id existente.
      - Añadir una nueva página al final (append_page), devolviendo su page_id.
      - Forzar a disco (flush) y cerrar el archivo.

    No define la estructura interna de la página ni los registros. Solo maneja bytes.
    """

    def __init__(self, path: str, page_size: int = PAGE_SIZE_DEFAULT) -> None:
        self.path = os.path.abspath(path)
        self.page_size = int(page_size)

        # Asegurar directorio y archivo
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        if not os.path.exists(self.path):
            # crear archivo vacío
            with open(self.path, "wb"):
                pass

        # Abrir en modo lectura/escritura binaria
        self._f = open(self.path, "r+b", buffering=0)

        # Si el tamaño no es múltiplo del tamaño de página, rellenar con ceros
        size = os.path.getsize(self.path)
        remainder = size % self.page_size
        if remainder != 0:
            padding = self.page_size - remainder
            self._f.seek(0, io.SEEK_END)
            self._f.write(b"\x00" * padding)
            self._f.flush()
            os.fsync(self._f.fileno())

    def file_size(self) -> int:
        return os.path.getsize(self.path)

    def page_count(self) -> int:
        return self.file_size() // self.page_size

    def read_page(self, page_id: int) -> bytes:
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
        """Añade una página al final del archivo. Si data es None, se escribe una página de ceros.
        Si data tiene menos de page_size bytes, se rellena con ceros a la derecha. Si tiene más, error.
        Retorna el nuevo page_id.
        """
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
        self._f.flush()
        os.fsync(self._f.fileno())

    def close(self) -> None:
        try:
            self.flush()
        finally:
            self._f.close()

    def __enter__(self) -> "DiskManager":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# --- Serialización simple (JSON + prefijo de longitud de 4 bytes) ---

def obj_to_bytes(obj: Any) -> bytes:
    return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def bytes_to_obj(data: bytes) -> Any:
    return json.loads(data.decode("utf-8"))


def pack_record(obj: Any) -> bytes:
    payload = obj_to_bytes(obj)
    return struct.pack("<I", len(payload)) + payload


def unpack_records(buffer: bytes) -> Tuple[List[Any], int]:
    """Desempaqueta múltiples registros de un buffer de bytes.
    Cada registro es: [4 bytes longitud little-endian] [payload JSON UTF-8].
    Retorna (registros, bytes_consumidos).
    """
    records: List[Any] = []
    offset = 0
    total = len(buffer)
    while offset + 4 <= total:
        (length,) = struct.unpack_from("<I", buffer, offset)
        # Si la longitud es 0, asumimos padding de ceros y detenemos el parseo
        if length == 0:
            break
        # Si el registro excede el buffer disponible, es incompleto; detenemos
        if offset + 4 + length > total:
            break
        start = offset + 4
        end = start + length
        payload = buffer[start:end]
        records.append(bytes_to_obj(payload))
        offset = end
    return records, offset


def get_io_counters() -> Tuple[int, int]:
    """Devuelve (disk_reads, disk_writes)."""
    return disk_reads, disk_writes


def reset_io_counters() -> None:
    """Reinicia los contadores de I/O a cero."""
    global disk_reads, disk_writes
    disk_reads = 0
    disk_writes = 0
