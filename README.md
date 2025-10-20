# Proyecto BD2 – Motor de BD + API + Frontend (Docker)

Este proyecto implementa un pequeño motor de base de datos con índices, expuesto vía API FastAPI y un frontend Next.js para operar con SQL y CSV. Incluye despliegue con Docker Compose, soporte de múltiples tipos de índices (BTREE, ISAM, AVL, HASH, RTREE) y consultas espaciales por SQL.

## Arquitectura rápida

- Backend: FastAPI + Uvicorn (`api/`), motor en `storage/`, `core/`, `indexes/` y parser SQL en `parser/`.
- Frontend: Next.js 14 (directorio `auth-app/`) con autenticación simple y un editor de SQL.
- Persistencia: archivos bajo `data/` (montado como volumen Docker).
- Métricas: módulo `metrics/` con contadores y tiempos por operación (I/O, índices, etc.).

## Índices soportados y operaciones

- BTREE: igualdad y rango (search, range).
- ISAM: estructura estática por páginas; igualdad y rango; útil para lotes y lecturas.
- AVL: árbol balanceado; igualdad y rango.
- HASH (extensible): igualdad.
- RTREE: espacial en 2D sobre columnas `ARRAY_FLOAT` (p. ej. `[lat, lon]`); soporta búsquedas por radio y KNN.

## SQL soportado (vía `POST /users/{user}/databases/{db}/query`)

- CREATE TABLE con columnas, tipos y opciones por columna:
	- Tipos: `INT`, `FLOAT`, `DATE`, `VARCHAR[n]`, `ARRAY[FLOAT]`.
	- Clave primaria: `KEY` en la columna.
	- Índice: `INDEX {BTREE|ISAM|AVL|HASH|RTREE}` en la columna.
- INSERT:
	- `INSERT INTO t VALUES (v1, v2, ...)` (orden según schema), o
	- `INSERT INTO t (c1, c2) VALUES (v1, v2)`.
- SELECT:
	- `SELECT * FROM t WHERE col = value`
	- `SELECT * FROM t WHERE col BETWEEN a AND b`
	- Espacial: `SELECT * FROM t WHERE NEAR(col, [lat, lon]) RADIUS r`
	- Espacial: `SELECT * FROM t WHERE KNN(col, [lat, lon]) K k`
- DELETE: `DELETE FROM t WHERE col = value`

Notas importantes:
- La sintaxis antigua `CREATE TABLE ... FROM FILE` ya no está soportada. Usa el uploader CSV del frontend o `POST /tables/{table}/load-csv`.
- Las columnas con `PRIMARY KEY` y tipos `INT/DATE/FLOAT` reciben índices BTREE sugeridos si no se declara uno explícito.

## Endpoints principales

- Salud: `GET /healthz`

- Usuarios (público + protegido):
	- `POST /users/register` → crear usuario
	- `POST /users/login` → obtener JWT
	- `GET /users/me` → usuario actual (Bearer)
	- `GET /users` → listar usuarios
	- `GET /users/{username}` → obtener usuario

- Bases de datos (Bearer): `/users/{user_id}/databases`
	- `POST ""` → crear DB
	- `GET ""` → listar DBs
	- `GET /{db_name}` → obtener DB
	- `DELETE /{db_name}` → borrar DB

- Tablas (Bearer): `/users/{user_id}/databases/{db}/tables`
	- `POST ""` → crear tabla (vía JSON del front; o usa SQL CREATE TABLE)
	- `GET ""` → listar tablas
	- `GET /{table}/schema` → schema
	- `GET /{table}/stats` → métricas de índices (agregadas)
	- `GET /{table}/indexes/{column}/stats` → métricas detalladas por índice

- Registros (Bearer): `/users/{user_id}/databases/{db}/tables/{table}/records`
	- `POST ""` → insertar un registro
	- `GET ""` → listar (scan)
	- `GET /search?column=col&key=val` → búsqueda por índice
	- `GET /range?column=col&begin_key=a&end_key=b` → rango por índice
	- Espacial: `POST /range-radius` body `{column, center:[lat,lon], radius}`
	- Espacial: `POST /knn` body `{column, center:[lat,lon], k}`

- SQL (Bearer): `/users/{user_id}/databases/{db}`
	- `POST /query` body `{ "sql": "..." }`

- Importación CSV (Bearer): `/users/{user_id}/databases/{db}/tables/{table}`
	- `POST /load-csv` multipart con `file`

Swagger UI: http://localhost:8000/docs

## Despliegue con Docker Compose (Windows)

Prerrequisitos:
- Docker Desktop (WSL2 habilitado) y ports libres 8000 (API) y 3000 (front).

Variables de entorno (opcional, `.env` en la raíz):
- `JWT_SECRET`: clave para firmar JWT.
- `CORS_ALLOWED_ORIGINS`: `http://localhost:3000,http://127.0.0.1:3000` (por defecto).
- `NEXT_PUBLIC_API_BASE_URL`: URL del backend para el front (por defecto `http://localhost:8000`).

Ejemplo `.env`:

```
JWT_SECRET=super-secret-key-change-me
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

Levantar servicios (en la raíz del repo):

```powershell
docker compose build ; docker compose up -d
```

Servicios:
- Backend: http://localhost:8000 (salud: `/healthz`, Swagger: `/docs`)
- Frontend: http://localhost:3000

Logs y control:

```powershell
docker compose ps
docker compose logs -f backend
docker compose logs -f frontend
```

Detener y limpiar:

```powershell
docker compose down
```

Persistencia: el volumen `./data` del host se monta en `/app/data` (backend) para conservar usuarios/DBs.

## Prueba funcional guiada desde el Frontend (todas los índices)

Objetivo: usando la UI (http://localhost:3000), realizar una secuencia de queries que ejercite BTREE, ISAM, AVL, HASH y RTREE, y visualizar métricas.

1) Registro y acceso
- Abra el front y registre un usuario (Register), luego Login.

2) Crear base de datos
- Click “New Database” → nombre: `demo` → seleccione `demo` en el selector.

3) BTREE (igualdad y rango)
- En “Query Input”, ejecute estas sentencias UNA POR UNA:

```sql
-- Tabla Personas con BTREE
CREATE TABLE Personas (
	id INT KEY INDEX BTREE,
	nombre VARCHAR[50] INDEX BTREE,
	edad INT INDEX BTREE,
	fecha DATE INDEX BTREE
);

INSERT INTO Personas (id, nombre, edad, fecha) VALUES (1, 'Alice', 30, '2024-01-01');
INSERT INTO Personas (id, nombre, edad, fecha) VALUES (2, 'Bob',   25, '2024-02-15');
INSERT INTO Personas (id, nombre, edad, fecha) VALUES (3, 'Caro',  40, '2024-03-20');

-- Igualdad
SELECT * FROM Personas WHERE id = 2;
-- Rango por edad
SELECT * FROM Personas WHERE edad BETWEEN 26 AND 45;
```

4) AVL (igualdad y rango)

```sql
CREATE TABLE Productos_AVL (
	id INT KEY,
	precio INT INDEX AVL,
	nombre VARCHAR[40]
);

INSERT INTO Productos_AVL (id, precio, nombre) VALUES (1, 100, 'Teclado');
INSERT INTO Productos_AVL (id, precio, nombre) VALUES (2,  50, 'Mouse');
INSERT INTO Productos_AVL (id, precio, nombre) VALUES (3, 300, 'Monitor');

SELECT * FROM Productos_AVL WHERE precio = 50;
SELECT * FROM Productos_AVL WHERE precio BETWEEN 60 AND 300;
```

5) ISAM (igualdad y rango)

```sql
CREATE TABLE Ventas_ISAM (
	id INT KEY,
	monto INT INDEX ISAM,
	fecha DATE INDEX ISAM
);

INSERT INTO Ventas_ISAM (id, monto, fecha) VALUES (1, 120, '2024-01-10');
INSERT INTO Ventas_ISAM (id, monto, fecha) VALUES (2, 350, '2024-02-05');
INSERT INTO Ventas_ISAM (id, monto, fecha) VALUES (3, 200, '2024-02-20');

SELECT * FROM Ventas_ISAM WHERE monto = 200;
SELECT * FROM Ventas_ISAM WHERE fecha BETWEEN '2024-02-01' AND '2024-02-28';
```

6) HASH (igualdad)

```sql
CREATE TABLE Emails_HASH (
	id INT KEY,
	email VARCHAR[64] INDEX HASH
);

INSERT INTO Emails_HASH (id, email) VALUES (1, 'a@x.com');
INSERT INTO Emails_HASH (id, email) VALUES (2, 'b@x.com');

SELECT * FROM Emails_HASH WHERE email = 'b@x.com';
```

7) RTREE (espacial: radio y KNN)

```sql
CREATE TABLE Lugares_RTREE (
	id INT KEY,
	coord ARRAY[FLOAT] INDEX RTREE,
	label VARCHAR[16]
);

INSERT INTO Lugares_RTREE (id, coord, label) VALUES (1, [19.4300, -99.1300], 'A');
INSERT INTO Lugares_RTREE (id, coord, label) VALUES (2, [19.4400, -99.1400], 'B');
INSERT INTO Lugares_RTREE (id, coord, label) VALUES (3, [19.4500, -99.1200], 'C');

-- Puntos cerca de [19.44, -99.14]
SELECT * FROM Lugares_RTREE WHERE NEAR(coord, [19.44, -99.14]) RADIUS 0.02;

-- 2 vecinos más cercanos a [19.44, -99.14]
SELECT * FROM Lugares_RTREE WHERE KNN(coord, [19.44, -99.14]) K 2;
```

Consejos:
- En el panel de resultados se muestran `execution_time_ms`, accesos a disco y métricas por índice usado.
- Para cargar datos masivos usa el botón “Import CSV” y el endpoint `/load-csv` (la tabla debe existir).

## Benchmarks (opcional)

Ejecuta el benchmark básico desde la raíz:

```powershell
python benchmarks/benchmark_basic.py
```

Los resultados quedan en `benchmarks/out/` (CSV/JSON y gráficos si `matplotlib` está instalado).


