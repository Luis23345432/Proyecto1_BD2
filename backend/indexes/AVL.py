import os
import struct
import csv
from dataclasses import dataclass

# helpers
def _fix_bytes(s: str, size: int) -> bytes:
    b = (s or "").encode("utf-8", errors="ignore")[:size]
    return b + b" " * (size - len(b))

def _unfix_str(b: bytes) -> str:
    return b.decode("utf-8", errors="ignore").rstrip(" ")

# Registro Employee
# int(4) + 30s + int(4) + 20s + 20s + 20s + float(4) + 10s = 112 bytes
EMP_FMT = "<i30si20s20s20sf10s"
EMP_SIZE = struct.calcsize(EMP_FMT)

@dataclass
class Employee:
    employee_id: int
    employee_name: str
    age: int
    country: str
    department: str
    position: str
    salary: float
    joining_date: str  # "DD/MM/YYYY"

    def pack(self) -> bytes:
        return struct.pack(
            EMP_FMT,
            self.employee_id,
            _fix_bytes(self.employee_name, 30),
            self.age,
            _fix_bytes(self.country, 20),
            _fix_bytes(self.department, 20),
            _fix_bytes(self.position, 20),
            float(self.salary),
            _fix_bytes(self.joining_date, 10),
        )

    @staticmethod
    def unpack(b: bytes) -> "Employee":
        (eid, name, age, country, dept, pos, sal, jdate) = struct.unpack(EMP_FMT, b)
        return Employee(
            eid,
            _unfix_str(name),
            age,
            _unfix_str(country),
            _unfix_str(dept),
            _unfix_str(pos),
            float(sal),
            _unfix_str(jdate),
        )

# Parseo
_ALIASES = {
    "employee_id": {"employeeid", "employee_id", "employee id", "id", "empid", "emp_id"},
    "employee_name": {"employeename", "employee_name", "employee name", "name", "fullname"},
    "age": {"age", "edad"},
    "country": {"country", "pais"},
    "department": {"department", "departamento", "dept"},
    "position": {"position", "cargo", "puesto", "jobtitle", "job"},
    "salary": {"salary", "salario", "sueldo", "pay"},
    "joining_date": {"joiningdate", "joining_date", "joining date", "hiredate", "fechaingreso"},
}

def _norm_key(s: str) -> str:
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())

def _mapeo_cabecera(reader_fieldnames):
    # Mapea cabeceras del CSV a los nombres esperados.
    if not reader_fieldnames:
        raise ValueError("El CSV no tiene encabezados.")
    normalized = {_norm_key(h): h for h in reader_fieldnames}
    header_map, missing = {}, []
    for expected, aliases in _ALIASES.items():
        found = None
        for alias in aliases:
            alias_norm = _norm_key(alias)
            if alias_norm in normalized:
                found = normalized[alias_norm]
                break
        if found is None:
            missing.append(expected)
        else:
            header_map[expected] = found
    if missing:
        available = ", ".join(reader_fieldnames)
        needed = ", ".join(missing)
        raise KeyError(
            f"No se encontraron columnas requeridas: [{needed}]. "
            f"Encabezados disponibles en tu CSV: {available}"
        )
    return header_map

def _columna_empleado(row, header_map) -> Employee:
    # Convierte una fila a Employee usando header_map.
    return Employee(
        int(row[header_map["employee_id"]]),
        str(row[header_map["employee_name"]] or ""),
        int(row[header_map["age"]]),
        str(row[header_map["country"]] or ""),
        str(row[header_map["department"]] or ""),
        str(row[header_map["position"]] or ""),
        float(row[header_map["salary"]]),
        str(row[header_map["joining_date"]] or ""),
    )

# Archivo AVL (persistente)
class AVLFile:

    HEADER_FMT = "<ii"
    HEADER_SIZE = struct.calcsize(HEADER_FMT)
    NODE_HDR_FMT = "<iiii"
    NODE_HDR_SIZE = struct.calcsize(NODE_HDR_FMT)
    NODE_SIZE = NODE_HDR_SIZE + EMP_SIZE

    FREE_KEY = -1  # marca de nodo libre

    def __init__(self, path="avl_file.bin"):
        self.path = path
        if not os.path.exists(self.path):
            with open(self.path, "wb") as f:
                # root = -1, free_head = -1
                f.write(struct.pack(self.HEADER_FMT, -1, -1))

    # helper archivo
    def _node_offset(self, idx: int) -> int:
        return self.HEADER_SIZE + idx * self.NODE_SIZE

    def _get_root_io(self, f) -> int:
        f.seek(0)
        root, _ = struct.unpack(self.HEADER_FMT, f.read(self.HEADER_SIZE))
        return root

    def _set_root_io(self, f, idx: int) -> None:
        # Actualiza solo root
        f.seek(0)
        root, free_head = struct.unpack(self.HEADER_FMT, f.read(self.HEADER_SIZE))
        f.seek(0)
        f.write(struct.pack(self.HEADER_FMT, idx, free_head))

    def _get_free_head_io(self, f) -> int:
        f.seek(0)
        _, free_head = struct.unpack(self.HEADER_FMT, f.read(self.HEADER_SIZE))
        return free_head

    def _set_free_head_io(self, f, free_head: int) -> None:
        f.seek(0)
        root, _ = struct.unpack(self.HEADER_FMT, f.read(self.HEADER_SIZE))
        f.seek(0)
        f.write(struct.pack(self.HEADER_FMT, root, free_head))

    def _node_count_io(self, f) -> int:
        size = os.path.getsize(self.path)
        return max(0, (size - self.HEADER_SIZE) // self.NODE_SIZE)

    # I/O de nodos
    def _read_node_io(self, f, cache: dict, idx: int):
        if idx < 0:
            return None
        if idx in cache:
            return cache[idx]
        f.seek(self._node_offset(idx))
        hdr = f.read(self.NODE_HDR_SIZE)
        if len(hdr) != self.NODE_HDR_SIZE:
            return None
        key, left, right, height = struct.unpack(self.NODE_HDR_FMT, hdr)
        data = f.read(EMP_SIZE)
        if len(data) != EMP_SIZE:
            return None
        node = {"idx": idx, "key": key, "left": left, "right": right, "height": height, "emp": Employee.unpack(data)}
        cache[idx] = node
        return node

    def _write_node_io(self, f, cache: dict, node: dict) -> None:
        idx = node["idx"]
        cache[idx] = node
        f.seek(self._node_offset(idx))
        f.write(struct.pack(self.NODE_HDR_FMT, node["key"], node["left"], node["right"], node["height"]))
        f.write(node["emp"].pack())

    def _append_node_io(self, f, emp: Employee, cache: dict) -> int:
        n = self._node_count_io(f)
        f.seek(0, os.SEEK_END)
        f.write(struct.pack(self.NODE_HDR_FMT, emp.employee_id, -1, -1, 1))
        f.write(emp.pack())
        cache[n] = {"idx": n, "key": emp.employee_id, "left": -1, "right": -1, "height": 1, "emp": emp}
        return n

    #  free list
    def _push_free_io(self, f, cache: dict, idx: int) -> None:
        # Empuja idx a la free list
        free_head = self._get_free_head_io(f)
        empty_emp = Employee(-1, "", 0, "", "", "", 0.0, "")
        node = {"idx": idx, "key": self.FREE_KEY, "left": free_head, "right": -1, "height": 0, "emp": empty_emp}
        self._write_node_io(f, cache, node)
        self._set_free_head_io(f, idx)

    def _pop_free_io(self, f, cache: dict) -> int:
        # Saca y devuelve el índice en la cabeza de la free list
        free_head = self._get_free_head_io(f)
        if free_head < 0:
            return -1
        head_node = self._read_node_io(f, cache, free_head)
        next_free = head_node["left"] if head_node else -1
        self._set_free_head_io(f, next_free)
        return free_head

    def _allocate_node_io(self, f, emp: Employee, cache: dict) -> int:
        # Reutiliza nodo de free list o hace append si no hay.
        idx = self._pop_free_io(f, cache)
        if idx >= 0:
            node = {"idx": idx, "key": emp.employee_id, "left": -1, "right": -1, "height": 1, "emp": emp}
            self._write_node_io(f, cache, node)
            return idx
        return self._append_node_io(f, emp, cache)

    #  altura y balance
    def _height_io(self, f, cache: dict, idx: int) -> int:
        if idx < 0:
            return 0
        n = self._read_node_io(f, cache, idx)
        return n["height"] if n else 0

    def _update_height_io(self, f, cache: dict, idx: int) -> None:
        n = self._read_node_io(f, cache, idx)
        if not n:
            return
        lh = self._height_io(f, cache, n["left"])
        rh = self._height_io(f, cache, n["right"])
        h = 1 + (lh if lh > rh else rh)
        if h != n["height"]:
            n["height"] = h
            self._write_node_io(f, cache, n)

    def _balance_factor_io(self, f, cache: dict, idx: int) -> int:
        n = self._read_node_io(f, cache, idx)
        if not n:
            return 0
        return self._height_io(f, cache, n["left"]) - self._height_io(f, cache, n["right"])

    #  rotaciones
    def _rotate_left_io(self, f, cache: dict, z_idx: int) -> int:
        z = self._read_node_io(f, cache, z_idx)
        y_idx = z["right"]
        y = self._read_node_io(f, cache, y_idx)
        if y is None:
            return z_idx
        T2_idx = y["left"]
        y["left"] = z_idx
        z["right"] = T2_idx
        self._write_node_io(f, cache, z)
        self._write_node_io(f, cache, y)
        self._update_height_io(f, cache, z["idx"])
        self._update_height_io(f, cache, y["idx"])
        return y["idx"]

    def _rotate_right_io(self, f, cache: dict, z_idx: int) -> int:
        z = self._read_node_io(f, cache, z_idx)
        y_idx = z["left"]
        y = self._read_node_io(f, cache, y_idx)
        if y is None:
            return z_idx
        T3_idx = y["right"]
        y["right"] = z_idx
        z["left"] = T3_idx
        self._write_node_io(f, cache, z)
        self._write_node_io(f, cache, y)
        self._update_height_io(f, cache, z["idx"])
        self._update_height_io(f, cache, y["idx"])
        return y["idx"]

    # inserción
    def _insert_rec_io(self, f, cache: dict, idx: int, emp: Employee) -> int:
        if idx < 0:
            return self._allocate_node_io(f, emp, cache)
        node = self._read_node_io(f, cache, idx)
        if not node:
            return idx
        if emp.employee_id < node["key"]:
            node["left"] = self._insert_rec_io(f, cache, node["left"], emp)
            self._write_node_io(f, cache, node)
        elif emp.employee_id > node["key"]:
            node["right"] = self._insert_rec_io(f, cache, node["right"], emp)
            self._write_node_io(f, cache, node)
        else:
            print(f"❌ Error: ya existe un empleado con ID {emp.employee_id}. No se insertó.")
            return idx
        self._update_height_io(f, cache, idx)
        bal = self._balance_factor_io(f, cache, idx)
        # LL
        if bal > 1:
            left = self._read_node_io(f, cache, node["left"])
            if left and emp.employee_id < left["key"]:
                return self._rotate_right_io(f, cache, idx)
            node["left"] = self._rotate_left_io(f, cache, node["left"])  # LR
            self._write_node_io(f, cache, node)
            return self._rotate_right_io(f, cache, idx)
        # RR
        if bal < -1:
            right = self._read_node_io(f, cache, node["right"])
            if right and emp.employee_id > right["key"]:
                return self._rotate_left_io(f, cache, idx)
            node["right"] = self._rotate_right_io(f, cache, node["right"])  # RL
            self._write_node_io(f, cache, node)
            return self._rotate_left_io(f, cache, idx)
        return idx

    def insert(self, emp: Employee) -> None:
        with open(self.path, "rb+") as f:
            cache = {}
            root = self._get_root_io(f)
            new_root = self._insert_rec_io(f, cache, root, emp)
            if new_root != root:
                self._set_root_io(f, new_root)

    # búsqueda
    def search(self, key: int) -> Employee | None:
        with open(self.path, "rb") as f:
            cache = {}
            idx = self._get_root_io(f)
            while idx >= 0:
                n = self._read_node_io(f, cache, idx)
                if not n:
                    break
                if key == n["key"]:
                    return n["emp"]
                idx = n["left"] if key < n["key"] else n["right"]
            return None

    # carga CSV
    def build_from_csv(self, csv_path: str) -> dict:
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"No existe el CSV: {csv_path}")
        # resetear archivo (root = -1, free_head = -1)
        with open(self.path, "wb") as f:
            f.write(struct.pack(self.HEADER_FMT, -1, -1))
        # detectar delimitador simple
        with open(csv_path, "r", encoding="utf-8", newline="") as ff:
            sample = ff.read(2048)
            delim = max([",", ";", "\t", "|"], key=lambda c: sample.count(c))
        count = 0
        with open(csv_path, newline="", encoding="utf-8") as ff:
            reader = csv.DictReader(ff, delimiter=delim)
            header_map = _mapeo_cabecera(reader.fieldnames)
            for r in reader:
                self.insert(_columna_empleado(r, header_map))
                count += 1
        return {"delimiter": delim, "headers_detected": header_map, "count": count}

    # eliminación
    def _min_idx_io(self, f, cache: dict, idx: int) -> int:
        cur = idx
        while True:
            n = self._read_node_io(f, cache, cur)
            if not n or n["left"] < 0:
                return cur
            cur = n["left"]

    def _delete_rec_io(self, f, cache: dict, idx: int, key: int) -> int:
        # Elimina 'key' en el subárbol con raíz 'idx' y devuelve la nueva raíz del subárbol.
        if idx < 0:
            return idx
        node = self._read_node_io(f, cache, idx)
        if not node:
            return idx
        if key < node["key"]:
            node["left"] = self._delete_rec_io(f, cache, node["left"], key)
            self._write_node_io(f, cache, node)
        elif key > node["key"]:
            node["right"] = self._delete_rec_io(f, cache, node["right"], key)
            self._write_node_io(f, cache, node)
        else:
            # Encontrado
            left_idx, right_idx = node["left"], node["right"]
            if left_idx < 0 or right_idx < 0:
                # 0 o 1 hijo: este nodo sale del árbol ----> empujar a free list
                self._push_free_io(f, cache, idx)
                return right_idx if left_idx < 0 else left_idx
            # 2 hijos: copiar sucesor y eliminarlo recursivamente
            succ_idx = self._min_idx_io(f, cache, right_idx)
            succ = self._read_node_io(f, cache, succ_idx)
            node["key"] = succ["key"]
            node["emp"] = succ["emp"]
            self._write_node_io(f, cache, node)
            node["right"] = self._delete_rec_io(f, cache, right_idx, succ["key"])
            self._write_node_io(f, cache, node)
        self._update_height_io(f, cache, idx)
        bal = self._balance_factor_io(f, cache, idx)
        # LL
        if bal > 1:
            left = self._read_node_io(f, cache, node["left"])
            if left and self._balance_factor_io(f, cache, left["idx"]) < 0:
                node["left"] = self._rotate_left_io(f, cache, node["left"])  # LR
                self._write_node_io(f, cache, node)
            return self._rotate_right_io(f, cache, idx)
        # RR
        if bal < -1:
            right = self._read_node_io(f, cache, node["right"])
            if right and self._balance_factor_io(f, cache, right["idx"]) > 0:
                node["right"] = self._rotate_right_io(f, cache, node["right"])  # RL
                self._write_node_io(f, cache, node)
            return self._rotate_left_io(f, cache, idx)
        return idx

    def delete(self, key: int) -> bool:
        # Elimina Employee_ID == key. Devuelve True si existía y marca espacio como libre
        with open(self.path, "rb+") as f:
            cache = {}
            root = self._get_root_io(f)
            # comprobar existencia
            found, idx = False, root
            while idx >= 0:
                n = self._read_node_io(f, cache, idx)
                if not n:
                    break
                if key == n["key"]:
                    found = True
                    break
                idx = n["left"] if key < n["key"] else n["right"]
            if not found:
                return False
            cache.clear()
            new_root = self._delete_rec_io(f, cache, root, key)
            if new_root != root:
                self._set_root_io(f, new_root)
            return True

    # búsqueda por rango
    def _range_rec_io(self, f, cache: dict, idx: int, lo: int, hi: int, out: list) -> None:
        if idx < 0:
            return
        n = self._read_node_io(f, cache, idx)
        if not n:
            return
        if lo < n["key"]:
            self._range_rec_io(f, cache, n["left"], lo, hi, out)
        if lo <= n["key"] <= hi:
            out.append(n["emp"])
        if hi > n["key"]:
            self._range_rec_io(f, cache, n["right"], lo, hi, out)

    def range_search(self, lo: int, hi: int) -> list[Employee]:
        # Devuelve empleados con Employee_ID, ordenados por ID.
        if lo > hi:
            lo, hi = hi, lo
        with open(self.path, "rb") as f:
            cache = {}
            root = self._get_root_io(f)
            res: list[Employee] = []
            self._range_rec_io(f, cache, root, lo, hi, res)
            return res
