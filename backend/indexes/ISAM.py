import struct
import os
import csv
import pytest

BLOCK_FACTOR = 60  # 20 registros por page


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
            self.producto[:30].ljust(30).encode(),
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
    FORMAT_HEADER = 'ii?'
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_PAGE = SIZE_HEADER + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=[], next_page=-1, is_empty=False):
        self.records = records
        self.next_page = next_page
        self.is_empty = is_empty

    def pack(self) -> bytes:
        header_data = struct.pack(self.FORMAT_HEADER, len(self.records),
                                  self.next_page, self.is_empty)
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
        size, next_page, is_empty = struct.unpack(Page.FORMAT_HEADER,
                                                  data[:Page.SIZE_HEADER])
        offset = Page.SIZE_HEADER
        records = []
        for i in range(size):
            record_data = data[offset: offset + Record.SIZE_OF_RECORD]
            record = Record.unpack(record_data)
            records.append(record)
            offset += Record.SIZE_OF_RECORD
        return Page(records, next_page, is_empty)

    def count_active_records(self):
        return sum(1 for r in self.records if not r.deleted)

    def get_active_records(self):
        return [r for r in self.records if not r.deleted]


class DataFile:
    def __init__(self, file_name):
        self.file_name = file_name

    def add(self, record: Record):
        if not os.path.exists(self.file_name):
            with open(self.file_name, 'wb') as file:
                new_page = Page([record])
                file.write(new_page.pack())
            return

        with open(self.file_name, 'r+b') as file:
            file.seek(0, 2)
            filesize = file.tell()
            pos_last_page = filesize - Page.SIZE_OF_PAGE
            file.seek(pos_last_page, 0)
            page = Page.unpack(file.read(Page.SIZE_OF_PAGE))

            if len(page.records) < BLOCK_FACTOR:
                page.records.append(record)
                file.seek(pos_last_page, 0)
                file.write(page.pack())
            else:
                file.seek(0, 2)
                new_page = Page([record])
                file.write(new_page.pack())

    def scanAll(self):
        with open(self.file_name, 'rb') as file:
            file.seek(0, 2)
            numPages = file.tell() // Page.SIZE_OF_PAGE
            file.seek(0, 0)
            for i in range(numPages):
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                active_count = page.count_active_records()
                page_status = "[empty]" if page.is_empty else f"[{active_count} records]"
                next_info = f" -> page {page.next_page}" if page.next_page != -1 else ""
                print(f"page {i + 1} {page_status}{next_info}")
                for record in page.get_active_records():
                    print(record)

    def num_pages(self):
        if not os.path.exists(self.file_name):
            return 0
        return os.path.getsize(self.file_name) // Page.SIZE_OF_PAGE

    def pos_of_page(self, page_no: int) -> int:
        return page_no * Page.SIZE_OF_PAGE

    def read_page(self, page_no: int) -> Page:
        with open(self.file_name, "rb") as f:
            f.seek(self.pos_of_page(page_no))
            return Page.unpack(f.read(Page.SIZE_OF_PAGE))

    def write_page(self, page_no: int, page: Page):
        with open(self.file_name, "r+b") as f:
            f.seek(self.pos_of_page(page_no))
            f.write(page.pack())

    def append_page(self, page: Page) -> int:
        with open(self.file_name, "ab") as f:
            f.write(page.pack())
        return self.num_pages() - 1

    def find_empty_page(self) -> int:
        with open(self.file_name, "rb") as f:
            num_pages = self.num_pages()
            for page_no in range(num_pages):
                f.seek(page_no * Page.SIZE_OF_PAGE)
                page = Page.unpack(f.read(Page.SIZE_OF_PAGE))
                if page.is_empty:
                    return page_no
        return -1


class IndexFile:
    HEADER_FORMAT = "I"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    ENTRY_FORMAT = "II"
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)

    def __init__(self, file_name, max_keys=20):
        self.file_name = file_name
        self.entries = []
        self.max_keys = max_keys
        self.is_fixed = False

    def build_from_data(self, data: DataFile):
        if self.is_fixed:
            self._save()
            return

        self.entries.clear()
        with open(data.file_name, "rb") as file:
            num_pages = os.path.getsize(data.file_name) // Page.SIZE_OF_PAGE
            for page_no in range(num_pages):
                file.seek(page_no * Page.SIZE_OF_PAGE)
                page_data = file.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)
                active_records = page.get_active_records()
                if not active_records or page.is_empty:
                    continue
                first_id = active_records[0].id_venta
                self.entries.append((first_id, page_no))

        self.entries.sort(key=lambda x: x[0])

        if len(self.entries) >= self.max_keys:
            self._create_fixed_structure(data)

        self._save()

    def _create_fixed_structure(self, data: DataFile):
        if len(self.entries) <= self.max_keys:
            self.is_fixed = True
            return

        step = len(self.entries) / self.max_keys
        fixed_entries = []
        for i in range(self.max_keys):
            idx = int(i * step)
            if idx < len(self.entries):
                fixed_entries.append(self.entries[idx])

        if len(fixed_entries) < self.max_keys and self.entries:
            remaining = self.max_keys - len(fixed_entries)
            for i in range(remaining):
                if len(self.entries) > len(fixed_entries):
                    fixed_entries.append(self.entries[-(remaining - i)])

        self.entries = fixed_entries[:self.max_keys]
        self.is_fixed = True

    def _save(self):
        with open(self.file_name, "wb") as file:
            file.write(struct.pack("?", self.is_fixed))
            file.write(struct.pack(self.HEADER_FORMAT, len(self.entries)))
            for first_id, page_no in self.entries:
                file.write(struct.pack(self.ENTRY_FORMAT, first_id, page_no))

    def load(self):
        self.entries.clear()
        if not os.path.exists(self.file_name):
            return

        with open(self.file_name, "rb") as file:
            fixed_data = file.read(1)
            if fixed_data:
                self.is_fixed = struct.unpack("?", fixed_data)[0]

            header = file.read(self.HEADER_SIZE)
            if not header:
                return

            (count,) = struct.unpack(self.HEADER_FORMAT, header)
            for _ in range(count):
                chunk = file.read(self.ENTRY_SIZE)
                if not chunk:
                    break
                first_id, page_no = struct.unpack(self.ENTRY_FORMAT, chunk)
                self.entries.append((first_id, page_no))

    def pretty_print(self):
        self.load()
        print("index entries:")
        status = " (FIXED STRUCTURE)" if self.is_fixed else ""
        print(f"max keys: {self.max_keys}{status}")
        for idv, page in self.entries:
            print(f"  {idv} -> page {page}")

    def _lower_bound_page(self, key: int):
        self.load()
        if not self.entries:
            return None

        if self.entries[0][0] > key:
            return self.entries[0][1]

        lo, hi = 0, len(self.entries) - 1
        ans = 0
        while lo <= hi:
            mid = (lo + hi) // 2
            if self.entries[mid][0] <= key:
                ans = mid
                lo = mid + 1
            else:
                hi = mid - 1

        return self.entries[ans][1]

    def find_by_id(self, dataf: DataFile, id_venta: int):
        page_no = self._lower_bound_page(id_venta)
        if page_no is None:
            return None

        with open(dataf.file_name, "rb") as f:
            while page_no != -1:
                f.seek(page_no * Page.SIZE_OF_PAGE)
                page = Page.unpack(f.read(Page.SIZE_OF_PAGE))
                for rec in page.get_active_records():
                    if rec.id_venta == id_venta:
                        return rec
                page_no = page.next_page

        return None

    def insert(self, dataf: DataFile, rec: Record):
        self.load()

        if dataf.num_pages() == 0:
            dataf.add(rec)
            self.build_from_data(dataf)
            return

        key = rec.id_venta
        page_no = self._lower_bound_page(key)
        if page_no is None:
            page_no = 0

        page = dataf.read_page(page_no)

        if len(page.records) < BLOCK_FACTOR:
            page.records.append(rec)
            page.records.sort(key=lambda r: r.id_venta)
            dataf.write_page(page_no, page)
            if not self.is_fixed:
                self.build_from_data(dataf)
            return

        merged = page.records + [rec]
        merged.sort(key=lambda r: r.id_venta)
        mid = len(merged) // 2
        left_records = merged[:mid]
        right_records = merged[mid:]

        original_next_page = page.next_page

        empty_page_no = dataf.find_empty_page()
        if empty_page_no != -1:
            new_page = Page(right_records, next_page=original_next_page, is_empty=False)
            dataf.write_page(empty_page_no, new_page)
            new_page_no = empty_page_no
        else:
            new_page = Page(right_records, next_page=original_next_page)
            new_page_no = dataf.append_page(new_page)

        page_left = Page(left_records, next_page=new_page_no)
        dataf.write_page(page_no, page_left)

        if not self.is_fixed:
            self.build_from_data(dataf)

    def delete_by_id(self, dataf: DataFile, id_venta: int):
        page_no = self._lower_bound_page(id_venta)
        if page_no is None:
            return False

        with open(dataf.file_name, "r+b") as f:
            while page_no != -1:
                f.seek(page_no * Page.SIZE_OF_PAGE)
                page = Page.unpack(f.read(Page.SIZE_OF_PAGE))

                for i, rec in enumerate(page.records):
                    if rec.id_venta == id_venta and not rec.deleted:
                        page.records[i].deleted = True
                        if page.count_active_records() == 0:
                            page.is_empty = True
                            print(f"page {page_no} now empty")
                        f.seek(page_no * Page.SIZE_OF_PAGE)
                        f.write(page.pack())
                        if not self.is_fixed:
                            self.build_from_data(dataf)
                        return True

                page_no = page.next_page

        return False


def build_data_and_index_from_csv(csv_path: str, dataf: DataFile, idxf: IndexFile):
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

    rows.sort(key=lambda rec: rec.id_venta)

    if os.path.exists(dataf.file_name):
        os.remove(dataf.file_name)

    for rec in rows:
        dataf.add(rec)

    idxf.build_from_data(dataf)


# FIXTURES PARA PYTEST
@pytest.fixture
def dataf():
    df = DataFile('test_sales_data.dat')
    yield df
    # Cleanup
    if os.path.exists(df.file_name):
        os.remove(df.file_name)


@pytest.fixture
def idxf():
    idx = IndexFile('test_sales_index.idx', max_keys=20)
    yield idx
    # Cleanup
    if os.path.exists(idx.file_name):
        os.remove(idx.file_name)


@pytest.fixture
def setup_data(dataf, idxf):
    """Fixture que carga datos iniciales para los tests"""
    if os.path.exists('sales_dataset_unsorted.csv'):
        build_data_and_index_from_csv('sales_dataset_unsorted.csv', dataf, idxf)
    return dataf, idxf


# TESTS PARA PYTEST
def test_estructura_basica(setup_data):
    dataf, idxf = setup_data
    print("\ntest estructura basica")
    print(f"total pages: {dataf.num_pages()}")
    assert dataf.num_pages() > 0


def test_busqueda_puntual(setup_data):
    dataf, idxf = setup_data
    print("\ntest search by id")
    test_ids = [1, 5, 10, 100, 9999]
    for id_test in test_ids:
        result = idxf.find_by_id(dataf, id_test)
        if result:
            print(f"found id {id_test}: {result.producto}")


def test_insercion(setup_data):
    dataf, idxf = setup_data
    print("\ntest insert")
    pages_antes = dataf.num_pages()

    nuevos_registros = [
        Record(999991, "test product 1", 1, 10.50, "01/01/2025"),
        Record(999992, "test product 2", 2, 15.75, "02/01/2025"),
    ]

    for nuevo in nuevos_registros:
        idxf.insert(dataf, nuevo)
        encontrado = idxf.find_by_id(dataf, nuevo.id_venta)
        assert encontrado is not None


def test_eliminacion_logica(setup_data):
    dataf, idxf = setup_data
    print("\ntest delete")
    test_ids = [1, 2, 3]

    for id_test in test_ids:
        if idxf.find_by_id(dataf, id_test):
            result = idxf.delete_by_id(dataf, id_test)
            if result:
                assert idxf.find_by_id(dataf, id_test) is None


def test_reutilizacion_paginas(setup_data):
    dataf, idxf = setup_data
    print("\ntest page reuse")
    pages_antes = dataf.num_pages()

    nuevo = Record(500000, "reused 1", 1, 5.00, "01/02/2025")
    idxf.insert(dataf, nuevo)

    assert idxf.find_by_id(dataf, nuevo.id_venta) is not None


def test_indice_consistencia(setup_data):
    dataf, idxf = setup_data
    print("\ntest index consistency")
    idxf.load()

    if len(idxf.entries) > 1:
        es_ordenado = all(idxf.entries[i][0] <= idxf.entries[i + 1][0]
                          for i in range(len(idxf.entries) - 1))
        assert es_ordenado


def test_encadenamiento_paginas(setup_data):
    dataf, idxf = setup_data
    print("\ntest page chaining")

    registros_masivos = []
    for i in range(10):
        nuevo_id = 800000 + i
        registros_masivos.append(Record(nuevo_id, f"chained {i}", 1, 10.0 + i, "01/03/2025"))

    for rec in registros_masivos:
        idxf.insert(dataf, rec)

    encontrados = sum(1 for rec in registros_masivos if idxf.find_by_id(dataf, rec.id_venta))
    assert encontrados == len(registros_masivos)


# MAIN PARA EJECUCIÓN DIRECTA
if __name__ == "__main__":
    dataf = DataFile('sales_data.dat')
    idxf = IndexFile('sales_index.idx', max_keys=20)

    print("building from csv...")
    build_data_and_index_from_csv('sales_dataset_unsorted.csv', dataf, idxf)

    print("\ndata pages:")
    dataf.scanAll()

    print("\nsparse index:")
    idxf.pretty_print()

    print("\nfinal state:")
    print(f"total pages: {dataf.num_pages()}")
    idxf.pretty_print()