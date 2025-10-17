import struct
import os
import csv

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
    FORMAT_HEADER = 'ii??'
    SIZE_HEADER = struct.calcsize(FORMAT_HEADER)
    SIZE_OF_PAGE = SIZE_HEADER + BLOCK_FACTOR * Record.SIZE_OF_RECORD

    def __init__(self, records=[], next_page=-1, is_empty=False, is_overflow=False):
        self.records = records
        self.next_page = next_page
        self.is_empty = is_empty
        self.is_overflow = is_overflow

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
    HEADER_FORMAT = "I?"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)

    def __init__(self, file_name):
        self.file_name = file_name
        self.free_list = FreeList(file_name + '.free')
        self.num_primary_pages = 0
        self.is_built = False

    def _read_header(self):
        if not os.path.exists(self.file_name):
            return

        with open(self.file_name, "rb") as f:
            header = f.read(self.HEADER_SIZE)
            if header and len(header) == self.HEADER_SIZE:
                self.num_primary_pages, self.is_built = struct.unpack(self.HEADER_FORMAT, header)

    def _write_header(self):
        with open(self.file_name, "r+b") as f:
            f.seek(0)
            f.write(struct.pack(self.HEADER_FORMAT, self.num_primary_pages, self.is_built))

    def build_initial_structure(self, records):
        if os.path.exists(self.file_name):
            os.remove(self.file_name)
        if os.path.exists(self.free_list.file_name):
            os.remove(self.free_list.file_name)

        records.sort(key=lambda r: r.id_venta)

        with open(self.file_name, "wb") as f:
            f.write(b'\x00' * self.HEADER_SIZE)

            page_records = []
            for rec in records:
                page_records.append(rec)
                if len(page_records) == BLOCK_FACTOR:
                    page = Page(page_records, is_overflow=False)
                    f.write(page.pack())
                    page_records = []
                    self.num_primary_pages += 1

            if page_records:
                page = Page(page_records, is_overflow=False)
                f.write(page.pack())
                self.num_primary_pages += 1

        self.is_built = True
        self._write_header()

    def num_pages(self):
        if not os.path.exists(self.file_name):
            return 0
        file_size = os.path.getsize(self.file_name)
        if file_size < self.HEADER_SIZE:
            return 0
        return (file_size - self.HEADER_SIZE) // Page.SIZE_OF_PAGE

    def pos_of_page(self, page_no: int) -> int:
        return self.HEADER_SIZE + (page_no * Page.SIZE_OF_PAGE)

    def read_page(self, page_no: int) -> Page:
        with open(self.file_name, "rb") as f:
            f.seek(self.pos_of_page(page_no))
            return Page.unpack(f.read(Page.SIZE_OF_PAGE))

    def write_page(self, page_no: int, page: Page):
        with open(self.file_name, "r+b") as f:
            f.seek(self.pos_of_page(page_no))
            f.write(page.pack())

    def append_overflow_page(self, page: Page) -> int:
        page.is_overflow = True
        with open(self.file_name, "ab") as f:
            f.write(page.pack())
        return self.num_pages() - 1

    def scanAll(self):
        self._read_header()
        with open(self.file_name, 'rb') as file:
            file.seek(self.HEADER_SIZE)
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
    HEADER_FORMAT = "II"
    HEADER_SIZE = struct.calcsize(HEADER_FORMAT)
    ENTRY_FORMAT = "II"
    ENTRY_SIZE = struct.calcsize(ENTRY_FORMAT)

    def __init__(self, file_name, max_keys=20):
        self.file_name = file_name
        self.entries = []
        self.max_keys = max_keys

    def build_from_primary_pages(self, data: DataFile):
        data._read_header()

        if not data.is_built:
            return

        self.entries.clear()

        with open(data.file_name, "rb") as f:
            f.seek(data.HEADER_SIZE)

            for page_no in range(data.num_primary_pages):
                f.seek(data.pos_of_page(page_no))
                page_data = f.read(Page.SIZE_OF_PAGE)
                page = Page.unpack(page_data)

                if not page.is_overflow:
                    active_records = page.get_active_records()
                    if active_records:
                        first_id = active_records[0].id_venta
                        self.entries.append((first_id, page_no))

        self.entries.sort(key=lambda x: x[0])

        if len(self.entries) > self.max_keys:
            self._create_second_level()

        self._save()

    def _create_second_level(self):
        if len(self.entries) <= self.max_keys:
            return

        step = len(self.entries) / self.max_keys
        second_level = []

        for i in range(self.max_keys):
            idx = int(i * step)
            if idx < len(self.entries):
                second_level.append(self.entries[idx])

        self.entries = second_level

    def _save(self):
        with open(self.file_name, "wb") as f:
            f.write(struct.pack(self.HEADER_FORMAT, len(self.entries), 0))
            for first_id, page_no in self.entries:
                f.write(struct.pack(self.ENTRY_FORMAT, first_id, page_no))

    def load(self):
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
        self.load()
        print("\nINDICE ESTATICO:")
        for idv, page in self.entries:
            print(f"  ID {idv:6d} -> pagina primaria {page}")

    def _find_primary_page(self, key: int) -> int:
        self.load()
        if not self.entries:
            return 0

        if key < self.entries[0][0]:
            return self.entries[0][1]

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
        dataf._read_header()
        page_no = self._find_primary_page(id_venta)

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
        dataf._read_header()

        if not dataf.is_built:
            return

        key = rec.id_venta
        primary_page_no = self._find_primary_page(key)

        current_page_no = primary_page_no
        prev_page_no = -1

        with open(dataf.file_name, "r+b") as f:
            while current_page_no != -1:
                f.seek(dataf.pos_of_page(current_page_no))
                current_page = Page.unpack(f.read(Page.SIZE_OF_PAGE))

                if len(current_page.records) < BLOCK_FACTOR:
                    current_page.records.append(rec)
                    current_page.records.sort(key=lambda r: r.id_venta)
                    f.seek(dataf.pos_of_page(current_page_no))
                    f.write(current_page.pack())
                    return

                if current_page.next_page == -1:
                    prev_page_no = current_page_no
                    break

                current_page_no = current_page.next_page

        dataf.free_list.load()
        new_overflow_page_no = dataf.free_list.get_free_page()

        if new_overflow_page_no != -1:
            new_page = Page([rec], is_empty=False, is_overflow=True)
            dataf.write_page(new_overflow_page_no, new_page)
        else:
            new_page = Page([rec], is_overflow=True)
            new_overflow_page_no = dataf.append_overflow_page(new_page)

        if prev_page_no != -1:
            prev_page = dataf.read_page(prev_page_no)
            prev_page.next_page = new_overflow_page_no
            dataf.write_page(prev_page_no, prev_page)

    def delete_by_id(self, dataf: DataFile, id_venta: int):
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

                        if page.count_active_records() == 0:
                            page.is_empty = True
                            if page.is_overflow:
                                dataf.free_list.load()
                                dataf.free_list.add_free_page(current_page_no)

                        f.seek(dataf.pos_of_page(current_page_no))
                        f.write(page.pack())
                        return True

                current_page_no = page.next_page

        return False


def build_isam_from_csv(csv_path: str, dataf: DataFile, idxf: IndexFile):
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

    dataf.build_initial_structure(rows)
    idxf.build_from_primary_pages(dataf)


if __name__ == "__main__":
    dataf = DataFile('sales_data.dat')
    idxf = IndexFile('sales_index.idx', max_keys=20)

    print("Construyendo ISAM desde CSV...")
    build_isam_from_csv('sales_dataset_unsorted.csv', dataf, idxf)

    print("\nPaginas de datos:")
    dataf.scanAll()

    print("\nIndice:")
    idxf.pretty_print()

    print("\nEstadisticas:")
    dataf._read_header()
    print(f"Paginas primarias: {dataf.num_primary_pages}")
    print(f"Total paginas: {dataf.num_pages()}")