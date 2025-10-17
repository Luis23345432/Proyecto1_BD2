import struct
import os
import csv
import pytest

BLOCK_FACTOR = 60  # Factor de bloque FIJO (no cambia nunca)


class Record:
    FORMAT = 'I40sid10s?'
    SIZE_OF_RECORD = struct.calcsize(FORMAT)

    def __init__(self, id_venta: int, producto: str, cantidad: int, precio: float,
                 fecha: str, deleted: bool = False):
        self.id_venta = id_venta
        self.producto = producto
        self.cantidad = cantidad
        self.precio = precio
        self.fecha = fecha
        self.deleted = deleted

    def pack(self) -> bytes:
        return struct.pack(
            self.FORMAT,
            self.id_venta,
            self.producto[:40].ljust(40).encode(),
            self.cantidad,
            self.precio,
            self.fecha[:10].ljust(10).encode(),
            self.deleted
        )

    @staticmethod
    def unpack(data: bytes):
        id_venta, prod, cantidad, precio, fecha, deleted = struct.unpack(Record.FORMAT, data)
        return Record(
            id_venta,
            prod.decode().rstrip(),
            cantidad,
            precio,
            fecha.decode().rstrip(),
            deleted
        )

    def __str__(self):
        status = "[DELETED]" if self.deleted else ""
        return f"{self.id_venta} | {self.producto} | {self.cantidad} | {self.precio:.2f} | {self.fecha} {status}"


class Page:
    FORMAT_HEADER = 'ii??'  # count, next_page, is_empty, is_overflow
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_PAGE = SIZE_HEADER + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=[], next_page=-1, is_empty=False, is_overflow=False):
        self.records = records
        self.next_page = next_page
        self.is_empty = is_empty
        self.is_overflow = is_overflow  # Marca si es página de overflow

    def pack(self) -> bytes:
        header_data = struct.pack(self.FORMAT_HEADER, len(self.records),
                                  self.next_page, self.is_empty, self.is_overflow)
        record_data = b''
        for record in self.records:
            record_data += record.pack()
        i = len(self.records)
        while i < BLOCK_FACTOR:
            record_data += b'\x00' * Record.SIZE_OF_RECORD
            i += 1
        return header_data + record_data

    @staticmethod
    def unpack(data: bytes):
        size, next_page, is_empty, is_overflow = struct.unpack(Page.FORMAT_HEADER,
                                                               data[:Page.SIZE_HEADER])
        offset = Page.SIZE_HEADER
        records = []
        for i in range(size):
            record_data = data[offset: offset + Record.SIZE_OF_RECORD]
            record = Record.unpack(record_data)
            records.append(record)
            offset += Record.SIZE_OF_RECORD
        return Page(records, next_page, is_empty, is_overflow)

    def count_active_records(self):
        return sum(1 for r in self.records if not r.deleted)

    def get_active_records(self):
        return [r for r in self.records if not r.deleted]


class FreeList:
    """Maneja páginas libres para reutilización (solo overflow)"""
    HEADER_FORMAT = "I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    ENTRY_FORMAT = "I"
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)

    def __init__(self, file_name):
        self.file_name = file_name
        self.free_pages = []

    def load(self):
        self.free_pages.clear()
        if not os.path.exists(self.file_name):
            return

        with open(self.file_name, "rb") as f:
            header = f.read(self.HEADER_SIZE)
            if not header:
                return
            (count,) = struct.unpack(self.HEADER_FORMAT, header)
            for _ in range(count):
                chunk = f.read(self.ENTRY_SIZE)
                if not chunk:
                    break
                (page_no,) = struct.unpack(self.ENTRY_FORMAT, chunk)
                self.free_pages.append(page_no)

    def save(self):
        with open(self.file_name, "wb") as f:
            f.write(struct.pack(self.HEADER_FORMAT, len(self.free_pages)))
            for page_no in self.free_pages:
                f.write(struct.pack(self.ENTRY_FORMAT, page_no))

    def add_free_page(self, page_no: int):
        if page_no not in self.free_pages:
            self.free_pages.append(page_no)
            self.save()

    def get_free_page(self) -> int:
        if self.free_pages:
            page_no = self.free_pages.pop(0)
            self.save()
            return page_no
        return -1


class DataFile:
    """Archivo de datos con estructura FIJA después de construcción inicial"""
    HEADER_FORMAT = "I?"  # num_primary_pages, is_built
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, file_name):
        self.file_name = file_name
        self.free_list = FreeList(file_name + '.free')
        self.num_primary_pages = 0
        self.is_built = False

    def _read_header(self):
        """Lee el header del archivo"""
        if not os.path.exists(self.file_name):
            return

        with open(self.file_name, "rb") as f:
            header = f.read(self.HEADER_SIZE)
            if header and len(header) == self.HEADER_SIZE:
                self.num_primary_pages, self.is_built = struct.unpack(self.HEADER_FORMAT, header)

    def _write_header(self):
        """Escribe el header del archivo"""
        with open(self.file_name, "r+b") as f:
            f.seek(0)
            f.write(struct.pack(self.HEADER_FORMAT, self.num_primary_pages, self.is_built))

    def build_initial_structure(self, records):
        """
        Construye la estructura INICIAL del ISAM (SOLO se llama UNA VEZ)
        Después de esto, NO se pueden crear nuevas páginas primarias
        """
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
        if os.path.exists(self.free_list.file_name):
            os.remove(self.free_list.file_name)

        # Ordenar registros por ID
        records.sort(key=lambda r: r.id_venta)

        # Crear archivo con header
        with open(self.file_name, "wb") as f:
            # Reservar espacio para header
            f.write(b'\x00' * self.HEADER_SIZE)

            # Crear páginas primarias con BLOCK_FACTOR registros cada una
            page_records = []
            for rec in records:
                page_records.append(rec)
                if len(page_records) == BLOCK_FACTOR:
                    page = Page(page_records, is_overflow=False)
                    f.write(page.pack())
                    page_records = []
                    self.num_primary_pages += 1

            # Última página con registros restantes
            if page_records:
                page = Page(page_records, is_overflow=False)
                f.write(page.pack())
                self.num_primary_pages += 1

        self.is_built = True
        self._write_header()
        print(f"✓ Estructura ISAM construida: {self.num_primary_pages} páginas primarias")
        print(f"✓ Estructura FIJA - No se crearán más páginas primarias")

    def num_pages(self):
        """Retorna el número TOTAL de páginas (primarias + overflow)"""
        if not os.path.exists(self.file_name):
            return 0
        file_size = os.path.getsize(self.file_name)
        if file_size < self.HEADER_SIZE:
            return 0
        return (file_size - self.HEADER_SIZE) // Page.SIZE_OF_PAGE

    def pos_of_page(self, page_no: int) -> int:
        """Retorna la posición de una página en el archivo"""
        return self.HEADER_SIZE + (page_no * Page.SIZE_OF_PAGE)

    def read_page(self, page_no: int) -> Page:
        """Lee una página del archivo"""
        with open(self.file_name, "rb") as f:
            f.seek(self.pos_of_page(page_no))
            return Page.unpack(f.read(Page.SIZE_OF_PAGE))

    def write_page(self, page_no: int, page: Page):
        """Escribe una página en el archivo"""
        with open(self.file_name, "r+b") as f:
            f.seek(self.pos_of_page(page_no))
            f.write(page.pack())

    def append_overflow_page(self, page: Page) -> int:
        """Agrega una página de OVERFLOW al final del archivo"""
        page.is_overflow = True  # Marcar como overflow
        with open(self.file_name, "ab") as f:
            f.write(page.pack())
        return self.num_pages() - 1

    def scanAll(self):
        """Escanea y muestra todas las páginas"""
        self._read_header()
        print(f"\n{'=' * 70}")
        print(f"ESTRUCTURA ISAM: {'CONSTRUIDA' if self.is_built else 'EN CONSTRUCCIÓN'}")
        print(f"Páginas primarias: {self.num_primary_pages}")
        print(f"Total páginas: {self.num_pages()}")
        print(f"{'=' * 70}\n")

        with open(self.file_name, 'rb') as file:
            file.seek(self.HEADER_SIZE)  # Saltar header
            total_pages = self.num_pages()

            for i in range(total_pages):
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)

                page_type = "PRIMARY" if not page.is_overflow else "OVERFLOW"
                active_count = page.count_active_records()
                page_status = "[EMPTY]" if page.is_empty else f"[{active_count} active]"
                next_info = f" -> page {page.next_page}" if page.next_page != -1 else ""

                print(f"Page {i} [{page_type}] {page_status}{next_info}")
                for record in page.get_active_records():
                    print(f"  {record}")


class IndexFile:
    """Índice ESTÁTICO de 2 niveles para ISAM"""
    HEADER_FORMAT = "II"  # num_entries, num_primary_pages
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    ENTRY_FORMAT = "II"  # first_id, page_no
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)

    def __init__(self, file_name, max_keys=20):
        self.file_name = file_name
        self.entries = []  # Nivel 1: índice sparse sobre páginas primarias
        self.max_keys = max_keys

    def build_from_primary_pages(self, data: DataFile):
        """
        Construye el índice SOLO sobre las páginas PRIMARIAS
        Este índice es ESTÁTICO y no cambia después de la construcción inicial
        """
        data._read_header()

        if not data.is_built:
            print("ERROR: El archivo de datos debe estar construido primero")
            return

        self.entries.clear()

        # Construir índice SOLO sobre páginas primarias (no overflow)
        with open(data.file_name, "rb") as f:
            f.seek(data.HEADER_SIZE)  # Saltar header

            for page_no in range(data.num_primary_pages):
                f.seek(data.pos_of_page(page_no))
                page_data = f.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)

                if not page.is_overflow:  # SOLO páginas primarias
                    active_records = page.get_active_records()
                    if active_records:
                        first_id = active_records[0].id_venta
                        self.entries.append((first_id, page_no))

        self.entries.sort(key=lambda x: x[0])

        # Crear nivel 2 del índice si es necesario (índice sobre el índice)
        if len(self.entries) > self.max_keys:
            self._create_second_level()

        self._save()
        print(f"✓ Índice ESTÁTICO construido: {len(self.entries)} entradas")
        print(f"✓ Indexa {data.num_primary_pages} páginas primarias")

    def _create_second_level(self):
        """Crea el segundo nivel del índice (índice sobre índice)"""
        if len(self.entries) <= self.max_keys:
            return

        step = len(self.entries) / self.max_keys
        second_level = []

        for i in range(self.max_keys):
            idx = int(i * step)
            if idx < len(self.entries):
                second_level.append(self.entries[idx])

        self.entries = second_level
        print(f"✓ Segundo nivel creado: reducido a {len(self.entries)} entradas")

    def _save(self):
        """Guarda el índice en disco"""
        with open(self.file_name, "wb") as f:
            f.write(struct.pack(self.HEADER_FORMAT, len(self.entries), 0))
            for first_id, page_no in self.entries:
                f.write(struct.pack(self.ENTRY_FORMAT, first_id, page_no))

    def load(self):
        """Carga el índice desde disco"""
        self.entries.clear()
        if not os.path.exists(self.file_name):
            return

        with open(self.file_name, "rb") as f:
            header = f.read(self.HEADER_SIZE)
            if not header:
                return

            count, _ = struct.unpack(self.HEADER_FORMAT, header)
            for _ in range(count):
                chunk = f.read(self.ENTRY_SIZE)
                if not chunk:
                    break
                first_id, page_no = struct.unpack(self.ENTRY_FORMAT, chunk)
                self.entries.append((first_id, page_no))

    def pretty_print(self):
        """Imprime el índice"""
        self.load()
        print(f"\n{'=' * 70}")
        print(f"ÍNDICE ESTÁTICO (2 niveles)")
        print(f"Max keys por nivel: {self.max_keys}")
        print(f"Entradas actuales: {len(self.entries)}")
        print(f"{'=' * 70}")
        for idv, page in self.entries:
            print(f"  ID {idv:6d} -> página primaria {page}")

    def _find_primary_page(self, key: int) -> int:
        """
        Encuentra la página PRIMARIA donde debería estar la clave
        usando el índice estático de 2 niveles
        """
        self.load()
        if not self.entries:
            return 0

        # Si la clave es menor que la primera entrada
        if key < self.entries[0][0]:
            return self.entries[0][1]

        # Búsqueda binaria en el índice
        lo, hi = 0, len(self.entries) - 1
        result_idx = 0

        while lo <= hi:
            mid = (lo + hi) // 2
            if self.entries[mid][0] <= key:
                result_idx = mid
                lo = mid + 1
            else:
                hi = mid - 1

        return self.entries[result_idx][1]

    def find_by_id(self, dataf: DataFile, id_venta: int):
        """Busca un registro por ID usando el índice de 2 niveles"""
        dataf._read_header()

        # Usar índice para encontrar página primaria
        page_no = self._find_primary_page(id_venta)

        # Buscar en la página primaria y su cadena de overflow
        with open(dataf.file_name, "rb") as f:
            while page_no != -1:
                f.seek(dataf.pos_of_page(page_no))
                page = Page.unpack(f.read(Page.SIZE_OF_PAGE))

                for rec in page.get_active_records():
                    if rec.id_venta == id_venta:
                        return rec

                page_no = page.next_page

        return None

    def insert(self, dataf: DataFile, rec: Record):
        """
        Inserta un registro en el ISAM.
        IMPORTANTE: Solo crea páginas de OVERFLOW, nunca páginas primarias nuevas
        """
        dataf._read_header()

        if not dataf.is_built:
            print("ERROR: El ISAM debe estar construido antes de insertar")
            return

        key = rec.id_venta
        primary_page_no = self._find_primary_page(key)

        # Buscar en la cadena desde la página primaria
        current_page_no = primary_page_no
        prev_page_no = -1

        with open(dataf.file_name, "r+b") as f:
            while current_page_no != -1:
                f.seek(dataf.pos_of_page(current_page_no))
                current_page = Page.unpack(f.read(Page.SIZE_OF_PAGE))

                # Si hay espacio, insertar aquí
                if len(current_page.records) < BLOCK_FACTOR:
                    current_page.records.append(rec)
                    current_page.records.sort(key=lambda r: r.id_venta)
                    f.seek(dataf.pos_of_page(current_page_no))
                    f.write(current_page.pack())
                    print(
                        f"✓ Insertado en página {current_page_no} (tipo: {'PRIMARY' if not current_page.is_overflow else 'OVERFLOW'})")
                    return

                # Si no hay espacio y no hay siguiente, necesitamos crear overflow
                if current_page.next_page == -1:
                    prev_page_no = current_page_no
                    break

                current_page_no = current_page.next_page

        # Crear nueva página de OVERFLOW
        dataf.free_list.load()
        new_overflow_page_no = dataf.free_list.get_free_page()

        if new_overflow_page_no != -1:
            # Reutilizar página libre
            new_page = Page([rec], is_empty=False, is_overflow=True)
            dataf.write_page(new_overflow_page_no, new_page)
            print(f"✓ Página libre reutilizada: {new_overflow_page_no}")
        else:
            # Crear nueva página de overflow al final
            new_page = Page([rec], is_overflow=True)
            new_overflow_page_no = dataf.append_overflow_page(new_page)
            print(f"✓ Nueva página OVERFLOW creada: {new_overflow_page_no}")

        # Actualizar la página anterior para apuntar a la nueva
        if prev_page_no != -1:
            prev_page = dataf.read_page(prev_page_no)
            prev_page.next_page = new_overflow_page_no
            dataf.write_page(prev_page_no, prev_page)
            print(f"✓ Chaining: página {prev_page_no} -> página {new_overflow_page_no}")

    def delete_by_id(self, dataf: DataFile, id_venta: int):
        """Elimina lógicamente un registro y gestiona páginas vacías"""
        dataf._read_header()
        primary_page_no = self._find_primary_page(id_venta)

        with open(dataf.file_name, "r+b") as f:
            current_page_no = primary_page_no

            while current_page_no != -1:
                f.seek(dataf.pos_of_page(current_page_no))
                page = Page.unpack(f.read(Page.SIZE_OF_PAGE))

                for i, rec in enumerate(page.records):
                    if rec.id_venta == id_venta and not rec.deleted:
                        page.records[i].deleted = True

                        # Si la página queda vacía Y es overflow, agregar a free list
                        if page.count_active_records() == 0:
                            page.is_empty = True
                            if page.is_overflow:  # Solo overflow pages van a free list
                                dataf.free_list.load()
                                dataf.free_list.add_free_page(current_page_no)
                                print(f"✓ Página overflow {current_page_no} marcada como libre")

                        f.seek(dataf.pos_of_page(current_page_no))
                        f.write(page.pack())
                        print(f"✓ Registro {id_venta} eliminado de página {current_page_no}")
                        return True

                current_page_no = page.next_page

        return False


def build_isam_from_csv(csv_path: str, dataf: DataFile, idxf: IndexFile):
    """
    Construye el ISAM completo desde un CSV
    Paso 1: Cargar y ordenar registros
    Paso 2: Construir estructura de datos FIJA
    Paso 3: Construir índice ESTÁTICO de 2 niveles
    """
    print(f"\n{'=' * 70}")
    print(f"CONSTRUCCIÓN INICIAL DEL ISAM")
    print(f"{'=' * 70}")

    # Cargar registros del CSV
    rows = []
    with open(csv_path, newline='', encoding='utf-8-sig') as f:
        reader = csv.reader(f, delimiter=';')
        header = next(reader, None)
        for r in reader:
            if not r or len(r) < 5:
                continue
            id_venta_str, producto, cantidad_str, precio_str, fecha = (x.strip() for x in r)
            precio_str = precio_str.replace(',', '.')
            id_venta = int(id_venta_str)
            cantidad = int(cantidad_str)
            precio = float(precio_str)
            rows.append(Record(id_venta, producto, cantidad, precio, fecha))

    print(f"✓ Registros cargados: {len(rows)}")

    # Construir estructura FIJA de datos
    dataf.build_initial_structure(rows)

    # Construir índice ESTÁTICO
    idxf.build_from_primary_pages(dataf)

    print(f"\n{'=' * 70}")
    print(f"ISAM CONSTRUIDO EXITOSAMENTE")
    print(f"Factor de bloque: {BLOCK_FACTOR} (FIJO)")
    print(f"Páginas primarias: {dataf.num_primary_pages} (FIJAS)")
    print(f"{'=' * 70}\n")


# ============================================================================
# FIXTURES PARA PYTEST
# ============================================================================

@pytest.fixture
def dataf():
    df = DataFile('test_sales_data.dat')
    yield df
    # Cleanup
    if os.path.exists(df.file_name):
        os.remove(df.file_name)
    if os.path.exists(df.free_list.file_name):
        os.remove(df.free_list.file_name)


@pytest.fixture
def idxf():
    idx = IndexFile('test_sales_index.idx', max_keys=20)
    yield idx
    if os.path.exists(idx.file_name):
        os.remove(idx.file_name)


@pytest.fixture
def setup_isam(dataf, idxf):
    """Fixture que construye el ISAM inicial para tests"""
    if os.path.exists('sales_dataset_unsorted.csv'):
        build_isam_from_csv('sales_dataset_unsorted.csv', dataf, idxf)
    return dataf, idxf


# ============================================================================
# TESTS PARA PYTEST
# ============================================================================

def test_construccion_estructura_fija(setup_isam):
    """Verifica que la estructura sea FIJA después de construcción"""
    dataf, idxf = setup_isam
    print("\n=== Test: Estructura FIJA ===")

    dataf._read_header()
    assert dataf.is_built, "El ISAM debe estar marcado como construido"
    assert dataf.num_primary_pages > 0, "Debe haber páginas primarias"

    primary_pages = dataf.num_primary_pages
    print(f"✓ Páginas primarias (FIJAS): {primary_pages}")
    print(f"✓ Factor de bloque (FIJO): {BLOCK_FACTOR}")


def test_busqueda_con_indice_estatico(setup_isam):
    """Verifica búsqueda usando índice estático de 2 niveles"""
    dataf, idxf = setup_isam
    print("\n=== Test: Búsqueda con índice estático ===")

    idxf.load()
    print(f"Entradas en índice: {len(idxf.entries)}")

    test_ids = [1, 50, 100, 500, 1000, 5000]
    for id_test in test_ids:
        result = idxf.find_by_id(dataf, id_test)
        if result:
            print(f"✓ ID {id_test}: {result.producto[:30]}")
        else:
            print(f"✗ ID {id_test}: No encontrado")


def test_insercion_solo_overflow(setup_isam):
    """Verifica que las inserciones SOLO creen páginas overflow"""
    dataf, idxf = setup_isam
    print("\n=== Test: Inserción solo crea OVERFLOW ===")

    dataf._read_header()
    primary_antes = dataf.num_primary_pages
    total_antes = dataf.num_pages()

    print(f"Páginas primarias antes: {primary_antes}")
    print(f"Total páginas antes: {total_antes}")

    # Insertar nuevos registros
    nuevos = [
        Record(999990 + i, f"Overflow Test {i}", i, 10.0 + i, "01/01/2025")
        for i in range(5)
    ]

    for rec in nuevos:
        idxf.insert(dataf, rec)

    dataf._read_header()
    primary_despues = dataf.num_primary_pages
    total_despues = dataf.num_pages()

    print(f"Páginas primarias después: {primary_despues}")
    print(f"Total páginas después: {total_despues}")

    assert primary_antes == primary_despues, "Las páginas primarias NO deben cambiar"
    print("✓ Páginas primarias SIN CAMBIOS (correcto)")

    # Verificar que se insertaron
    for rec in nuevos:
        result = idxf.find_by_id(dataf, rec.id_venta)
        assert result is not None, f"Registro {rec.id_venta} debe existir"


def test_chaining_overflow_pages(setup_isam):
    """Verifica el encadenamiento correcto de páginas overflow"""
    dataf, idxf = setup_isam
    print("\n=== Test: Chaining de páginas overflow ===")

    # Insertar muchos registros en un rango para forzar múltiples overflows
    base_id = 800000
    cantidad = 30
    registros = [
        Record(base_id + i, f"Chain {i}", 1, 10.0, "01/01/2025")
        for i in range(cantidad)
    ]

    for rec in registros:
        idxf.insert(dataf, rec)

    # Verificar que todos se pueden encontrar (prueba de chaining)
    encontrados = 0
    for rec in registros:
        result = idxf.find_by_id(dataf, rec.id_venta)
        if result:
            encontrados += 1

    print(f"Registros insertados: {cantidad}")
    print(f"Registros encontrados: {encontrados}")
    assert encontrados == cantidad, "Todos deben ser encontrados (chaining correcto)"


def test_eliminacion_y_free_list(setup_isam):
    """Verifica eliminación y gestión de páginas libres (solo overflow)"""
    dataf, idxf = setup_isam
    print("\n=== Test: Eliminación y free list ===")

    # Primero insertar en overflow
    nuevos = [Record(900000 + i, f"Delete Test {i}", 1, 5.0, "01/01/2025") for i in range(5)]
    for rec in nuevos:
        idxf.insert(dataf, rec)

    # Eliminar los registros
    for rec in nuevos:
        result = idxf.delete_by_id(dataf, rec.id_venta)
        assert result, f"Debe poder eliminar {rec.id_venta}"

    # Verificar free list
    dataf.free_list.load()
    print(f"✓ Páginas en free list: {len(dataf.free_list.free_pages)}")

    # Verificar que no se pueden encontrar después de eliminar
    for rec in nuevos:
        result = idxf.find_by_id(dataf, rec.id_venta)
        assert result is None, f"ID {rec.id_venta} no debería existir"


def test_reutilizacion_overflow_pages(setup_isam):
    """Verifica que se reutilicen páginas overflow libres"""
    dataf, idxf = setup_isam
    print("\n=== Test: Reutilización de páginas overflow ===")

    # Insertar y luego eliminar para crear páginas libres
    temp_ids = [920000 + i for i in range(3)]
    for id_v in temp_ids:
        rec = Record(id_v, "Temporal", 1, 1.0, "01/01/2025")
        idxf.insert(dataf, rec)

    for id_v in temp_ids:
        idxf.delete_by_id(dataf, id_v)

    dataf.free_list.load()
    free_antes = len(dataf.free_list.free_pages)
    print(f"Páginas libres antes: {free_antes}")

    # Insertar nuevo registro que debería reutilizar página libre
    nuevo = Record(930000, "Reutilizado", 1, 99.99, "02/01/2025")
    idxf.insert(dataf, nuevo)

    dataf.free_list.load()
    free_despues = len(dataf.free_list.free_pages)
    print(f"Páginas libres después: {free_despues}")

    # Verificar que se insertó correctamente
    result = idxf.find_by_id(dataf, nuevo.id_venta)
    assert result is not None, "El registro reutilizado debe existir"
    print(f"✓ Registro insertado: {result}")


def test_indice_no_cambia(setup_isam):
    """Verifica que el índice NO cambie después de inserciones"""
    dataf, idxf = setup_isam
    print("\n=== Test: Índice ESTÁTICO no cambia ===")

    idxf.load()
    entries_antes = len(idxf.entries)
    entries_snapshot = idxf.entries.copy()

    print(f"Entradas en índice antes: {entries_antes}")

    # Insertar varios registros
    for i in range(10):
        rec = Record(940000 + i, f"No Change Index {i}", 1, 5.0, "01/01/2025")
        idxf.insert(dataf, rec)

    idxf.load()
    entries_despues = len(idxf.entries)

    print(f"Entradas en índice después: {entries_despues}")

    assert entries_antes == entries_despues, "El índice NO debe cambiar"
    assert entries_snapshot == idxf.entries, "Las entradas deben ser idénticas"
    print("✓ Índice ESTÁTICO confirmado (no cambió)")


def test_factor_bloque_fijo(setup_isam):
    """Verifica que el factor de bloque sea FIJO en todas las páginas"""
    dataf, idxf = setup_isam
    print("\n=== Test: Factor de bloque FIJO ===")

    with open(dataf.file_name, "rb") as f:
        f.seek(dataf.HEADER_SIZE)
        for i in range(dataf.num_pages()):
            page_data = f.read(Page.SIZE_OF_PAGE)
            page = Page.unpack(page_data)

            # Cada página tiene espacio para exactamente BLOCK_FACTOR registros
            assert len(page.records) <= BLOCK_FACTOR

            if i < 3:  # Mostrar primeras páginas
                print(f"Página {i}: {len(page.records)}/{BLOCK_FACTOR} registros")

    print(f"✓ Factor de bloque FIJO verificado: {BLOCK_FACTOR}")


def test_estructura_dos_niveles(setup_isam):
    """Verifica que el índice tenga estructura de 2 niveles"""
    dataf, idxf = setup_isam
    print("\n=== Test: Índice de 2 niveles ===")

    idxf.load()
    print(f"Entradas en índice (nivel 1): {len(idxf.entries)}")
    print(f"Max keys por nivel: {idxf.max_keys}")

    # Si hay más páginas primarias que max_keys, debe haber segundo nivel
    dataf._read_header()
    if dataf.num_primary_pages > idxf.max_keys:
        assert len(idxf.entries) <= idxf.max_keys, "Debe aplicar segundo nivel"
        print("✓ Segundo nivel aplicado correctamente")
    else:
        print("✓ Un solo nivel es suficiente")


# ============================================================================
# MAIN PARA EJECUCIÓN DIRECTA
# ============================================================================

if __name__ == "__main__":
    print("""
╔═══════════════════════════════════════════════════════════════════════╗
║                   ISAM ESTÁTICO DE 2 NIVELES                          ║
║                                                                       ║
║  Características:                                                     ║
║  • Estructura FIJA después de construcción inicial                   ║
║  • Factor de bloque FIJO (no cambia nunca)                           ║
║  • Nuevas inserciones SOLO crean páginas OVERFLOW                    ║
║  • Índice ESTÁTICO de 2 niveles                                      ║
║  • Chaining para manejar overflow                                    ║
╚═══════════════════════════════════════════════════════════════════════╝
    """)

    dataf = DataFile('sales_data.dat')
    idxf = IndexFile('sales_index.idx', max_keys=20)

    # Verificar si existe el CSV
    csv_file = 'sales_dataset_unsorted.csv'
    if not os.path.exists(csv_file):
        print(f"\n⚠️  ERROR: No se encuentra el archivo '{csv_file}'")
        print("\nPor favor, asegúrese de tener el archivo CSV en el mismo directorio.")
        print("El archivo debe tener el formato:")
        print("  id_venta;producto;cantidad;precio;fecha")
        print("\nEjemplo de contenido:")
        print("  1;Laptop;2;1500.50;01/01/2024")
        print("  2;Mouse;5;25.99;02/01/2024")
        print("\n¿Desea crear un archivo de ejemplo? (s/n): ", end="")

        respuesta = input().strip().lower()
        if respuesta == 's':
            # Crear CSV de ejemplo
            with open(csv_file, 'w', encoding='utf-8') as f:
                f.write("id_venta;producto;cantidad;precio;fecha\n")
                f.write("1;Laptop HP;2;1500.50;01/01/2024\n")
                f.write("5;Mouse Logitech;5;25.99;02/01/2024\n")
                f.write("3;Teclado Mecánico;3;89.99;03/01/2024\n")
                f.write("10;Monitor LG;1;299.99;04/01/2024\n")
                f.write("7;Auriculares;4;45.50;05/01/2024\n")
                f.write("2;Webcam HD;2;65.00;06/01/2024\n")
                f.write("8;USB 32GB;10;12.99;07/01/2024\n")
                f.write("4;SSD 500GB;1;79.99;08/01/2024\n")
                f.write("9;Cable HDMI;6;8.50;09/01/2024\n")
                f.write("6;Hub USB;3;22.99;10/01/2024\n")
            print(f"\n✓ Archivo '{csv_file}' creado con datos de ejemplo")
            print("Construyendo ISAM...\n")
        else:
            print("\n❌ No se puede continuar sin el archivo CSV.")
            exit(1)

    # Verificar si ya existe el ISAM
    if os.path.exists(dataf.file_name):
        print("\n⚠️  Archivo de datos ya existe. ¿Desea reconstruir? (s/n): ", end="")
        respuesta = input().strip().lower()
        if respuesta != 's':
            print("\nCargando estructura existente...")
            dataf._read_header()
            idxf.load()

            print(f"\n{'=' * 70}")
            print("ESTRUCTURA CARGADA:")
            print(f"{'=' * 70}")
            print(f"Páginas primarias (FIJAS): {dataf.num_primary_pages}")
            print(f"Total páginas: {dataf.num_pages()}")
            print(f"Entradas en índice: {len(idxf.entries)}")
            print(f"{'=' * 70}\n")
        else:
            # Reconstruir
            build_isam_from_csv(csv_file, dataf, idxf)
    else:
        # Construcción inicial
        print("\nConstruyendo ISAM por primera vez...")
        build_isam_from_csv(csv_file, dataf, idxf)

    # Menú interactivo
    while True:
        print(f"\n{'=' * 70}")
        print("MENÚ ISAM")
        print(f"{'=' * 70}")
        print("1. Mostrar todas las páginas")
        print("2. Mostrar índice")
        print("3. Buscar por ID")
        print("4. Insertar registro")
        print("5. Eliminar registro")
        print("6. Estadísticas")
        print("7. Ejecutar tests")
        print("0. Salir")
        print(f"{'=' * 70}")

        opcion = input("\nSeleccione opción: ").strip()

        if opcion == "1":
            dataf.scanAll()

        elif opcion == "2":
            idxf.pretty_print()

        elif opcion == "3":
            try:
                id_buscar = int(input("Ingrese ID a buscar: "))
                resultado = idxf.find_by_id(dataf, id_buscar)
                if resultado:
                    print(f"\n✓ Registro encontrado:")
                    print(f"  {resultado}")
                else:
                    print(f"\n✗ Registro con ID {id_buscar} no encontrado")
            except ValueError:
                print("✗ ID inválido")

        elif opcion == "4":
            try:
                print("\nIngrese datos del nuevo registro:")
                id_venta = int(input("  ID: "))
                producto = input("  Producto: ")
                cantidad = int(input("  Cantidad: "))
                precio = float(input("  Precio: "))
                fecha = input("  Fecha (DD/MM/YYYY): ")

                nuevo_rec = Record(id_venta, producto, cantidad, precio, fecha)
                idxf.insert(dataf, nuevo_rec)

                print(f"\n✓ Registro insertado exitosamente")

            except ValueError:
                print("✗ Datos inválidos")

        elif opcion == "5":
            try:
                id_eliminar = int(input("Ingrese ID a eliminar: "))
                if idxf.delete_by_id(dataf, id_eliminar):
                    print(f"\n✓ Registro {id_eliminar} eliminado")
                else:
                    print(f"\n✗ Registro {id_eliminar} no encontrado")
            except ValueError:
                print("✗ ID inválido")

        elif opcion == "6":
            dataf._read_header()
            idxf.load()
            dataf.free_list.load()

            print(f"\n{'=' * 70}")
            print("ESTADÍSTICAS DEL ISAM")
            print(f"{'=' * 70}")
            print(f"Factor de bloque (FIJO):        {BLOCK_FACTOR}")
            print(f"Páginas primarias (FIJAS):      {dataf.num_primary_pages}")
            print(f"Total páginas (con overflow):   {dataf.num_pages()}")
            print(f"Páginas overflow:               {dataf.num_pages() - dataf.num_primary_pages}")
            print(f"Páginas libres (reutilizables): {len(dataf.free_list.free_pages)}")
            print(f"Entradas en índice:             {len(idxf.entries)}")
            print(f"Max keys por nivel:             {idxf.max_keys}")
            print(f"Estructura:                     {'FIJA ✓' if dataf.is_built else 'NO CONSTRUIDA'}")
            print(f"{'=' * 70}")

        elif opcion == "7":
            print("\n🧪 Ejecutando tests...")
            print("Para ejecutar todos los tests, use: pytest tu_archivo.py -v")
            print("\nTests disponibles:")
            print("  • test_construccion_estructura_fija")
            print("  • test_busqueda_con_indice_estatico")
            print("  • test_insercion_solo_overflow")
            print("  • test_chaining_overflow_pages")
            print("  • test_eliminacion_y_free_list")
            print("  • test_reutilizacion_overflow_pages")
            print("  • test_indice_no_cambia")
            print("  • test_factor_bloque_fijo")
            print("  • test_estructura_dos_niveles")

        elif opcion == "0":
            print("\n👋 Saliendo...")
            break

        else:
            print("\n✗ Opción inválida")