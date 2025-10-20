# Proyecto BD2 – Paso 2: DiskManager + Serialización

Este proyecto implementa un motor de base de datos por etapas. Este documento describe el Paso 2: DiskManager + Serialización.

## ¿Qué es DiskManager?

DiskManager es un componente de bajo nivel que administra páginas (bloques) de tamaño fijo (por defecto 4096 bytes) en un archivo de datos (por ejemplo, `data.dat` de una tabla). Sus responsabilidades:

- Abrir/crear el archivo de datos y asegurar que su tamaño sea múltiplo del tamaño de página.
- Leer páginas completas por `page_id` (offset = page_id * page_size).
- Escribir páginas completas en un `page_id` existente.
- Anexar nuevas páginas al final (append_page), devolviendo el nuevo `page_id`.
- Flush/sincronización a disco y cierre seguro.

Nota: DiskManager NO define la estructura interna de la página (eso se hace en el Paso 3 con DataFile + DataPage). Sólo opera con bytes.

## Serialización

Para serializar registros se incluye un esquema simple basado en JSON con prefijo de longitud:

- `pack_record(obj) -> bytes`: Serializa el objeto a JSON UTF-8 y lo precede con 4 bytes (little-endian) que indican la longitud del payload.
- `unpack_records(buffer) -> (records, bytes_consumidos)`: Lee sucesivos registros del buffer siguiendo el formato [longitud(4B) | payload] hasta agotar registros completos o encontrar uno incompleto.

Este formato permite empaquetar múltiples registros en una misma página y detecta delimitaciones de forma fiable.

## ¿Por qué en un archivo separado (disk_manager.py)?

- Separación de responsabilidades: `main.py` quedó para utilidades de sistema de archivos y metadatos del Paso 1. `disk_manager.py` encapsula el acceso binario de bajo nivel.
- Reutilización: el Paso 3 (DataFile + DataPage) y capas superiores usarán esta clase sin acoplarse al CLI.
- Testeabilidad: es más fácil unit-testear `DiskManager` por separado.

## Archivos relevantes

- `disk_manager.py`: implementación de `DiskManager`, `pack_record`, `unpack_records`.
- `main.py`: utilidades del Paso 1 (creación de usuarios/DB/tabla y JSONs). Actualizado para timestamps con timezone.
- `demo_disk_manager.py`: script de ejemplo para probar append/read y serialización.

## Cómo probar (PowerShell)

1) Crear estructura base (si no existe):

```powershell
python .\main.py demo_user sales_db customers
```

2) Ejecutar la demo del DiskManager:

```powershell
python .\demo_disk_manager.py
```

Salida esperada (similar):

```
page_id= 0
records= [{'id': 1, 'name': 'Alice'}, {'id': 2, 'name': 'Bob'}]
bytes_used= 54    # (valor aproximado; depende del JSON)
```

## Contrato y errores

- page_size: entero > 0; por defecto 4096.
- read_page/write_page: requieren `page_id` válido; si no, lanzan ValueError.
- write_page: exige exactamente `page_size` bytes.
- append_page: rellena con ceros si se pasan menos bytes; falla si se pasan más.
- flush: fuerza a OS y usa `fsync` para durabilidad.

## Siguientes pasos

- Paso 3 (DataFile + DataPage): definir layout interno de la página (cabecera, slots, etc.) y cómo se mapean los registros serializados a páginas.
- Añadir pruebas unitarias formales (pytest) y un banco de pruebas.

## Métricas y Benchmarking (Paso 18–19)

Este proyecto incluye un sistema de métricas ligero (`metrics/StatsManager`) para recolectar:

- Contadores (I/O, operaciones de índices, consultas de tabla, etc.)
- Tiempos acumulados por bloque usando `stats.timer(name)`

Instrumentación actual:

- `datafile.py`: page_count, read_page, write_page, append_page, insert_clustered
- Índices (`indexes/B+Tree.py`, `indexes/ISAM.py`, `indexes/AVL.py`): add/search/range/remove
- `storage/table.py`: insert/search/range/delete
- `engine.py`: create_table/insert/search/range/delete

Benchmark simple: `benchmarks/benchmark_basic.py`

1) Ejecutar desde la raíz del proyecto (Windows PowerShell):

```powershell
python benchmarks/benchmark_basic.py
```

2) Salida en `benchmarks/out/`:

- `metrics.csv` y `metrics.json` (métricas planas y snapshot)
- `counters.png` (si `matplotlib` está instalado; opcional)

## Backend FastAPI

Instalar dependencias (recomendado en un entorno virtual):

```powershell
pip install -r requirements.txt
```

Levantar el servidor de desarrollo:

```powershell
uvicorn api.app:app --reload
```

Endpoints principales (resumen):

- Usuarios:
	- POST /users
	- GET /users
	- GET /users/{user_id}
- Bases de datos:
	- POST /users/{user_id}/databases
	- GET /users/{user_id}/databases
	- GET /users/{user_id}/databases/{db_name}
	- DELETE /users/{user_id}/databases/{db_name}
- Tablas:
	- POST /users/{user_id}/databases/{db_name}/tables
	- GET /users/{user_id}/databases/{db_name}/tables
	- GET /users/{user_id}/databases/{db_name}/tables/{table_name}
	- GET /users/{user_id}/databases/{db_name}/tables/{table_name}/schema
	- GET /users/{user_id}/databases/{db_name}/tables/{table_name}/stats
- Registros:
	- POST /users/{user_id}/databases/{db_name}/tables/{table_name}/records
	- GET /users/{user_id}/databases/{db_name}/tables/{table_name}/records?limit=100&offset=0
- SQL:
	- POST /users/{user_id}/databases/{db_name}/query
- Importación CSV:
	- POST /users/{user_id}/databases/{db_name}/tables/{table_name}/load-csv

Swagger UI: http://127.0.0.1:8000/docs

## SQL DDL

El parser soporta una forma simple de DDL para `CREATE TABLE` con lista de columnas, tipos y claves/índices por columna:

- Tipos: `INT`, `VARCHAR[n]`, `DATE`, `ARRAY[FLOAT]` (para coordenadas `[lat, lon]`)
- Clave primaria: añadir `KEY` a la columna
- Índices: añadir `INDEX <BTREE|ISAM|AVL|HASH|RTREE>` a la columna

Ejemplo de creación con DDL vía endpoint `/query`:

```
POST /users/{user_id}/databases/{db_name}/query
{
	"sql": "CREATE TABLE Restaurantes ( id INT KEY, nombre VARCHAR[20] INDEX BTREE, fechaRegistro DATE, ubicacion ARRAY[FLOAT] INDEX RTREE );"
}
```

Notas:
- El CSV debe contener encabezados con nombres de columnas compatibles con el esquema.
- Para `ARRAY[FLOAT]` en CSV, coloque el par como un solo campo, por ejemplo `"[19.43, -99.13]"`.

Importación de CSV:
- Usa únicamente `POST /users/{user_id}/databases/{db_name}/tables/{table_name}/load-csv`.
- La tabla debe existir previamente (créala con `/tables` o con `CREATE TABLE` vía `/query`).

Importante: la sintaxis `CREATE TABLE ... FROM FILE` ya no está soportada. Si la usas, el servidor devolverá un error indicando que debes subir el CSV con el endpoint de carga.

### Índices por defecto (sugeridos)

Al crear una tabla, el sistema aplica una política de “índices sugeridos” para completar índices razonables cuando no se especifican explícitamente en el DDL:

- Si una columna es `PRIMARY KEY` o `UNIQUE` → se le asigna índice `BTREE`.
- Si el tipo de columna es `INT`, `DATE` o `FLOAT` → se le sugiere `BTREE`.
- Para `VARCHAR` (u otros no listados) → no se asigna índice por defecto, salvo que lo declares en el DDL (`INDEX BTREE|ISAM|AVL|HASH|RTREE`).

Importante:
- Esta sugerencia se ejecuta en el momento de crear la tabla dentro de `Database.create_table`, por lo que puede añadir índices por defecto incluso si ya definiste algunos índices explícitos en el DDL.
- Ejemplo: en `CREATE TABLE ... ( id INT KEY, nombre VARCHAR[20] INDEX BTREE, fechaRegistro DATE, ubicacion ARRAY[FLOAT] INDEX RTREE )`, la columna `fechaRegistro` es de tipo `DATE`, por lo que recibirá `BTREE` automáticamente además de los índices que declaraste para otras columnas.
- Si en algún momento prefieres no tener índice por defecto sobre ciertos tipos (como `DATE`), se puede ajustar la política en el código (`TableSchema.suggest_indexes`) o hacerla configurable en el futuro.

## Índice espacial RTREE (SQL + endpoints)

Creación vía SQL (endpoint `/query`):

```
POST /users/{user_id}/databases/{db_name}/query
{
	"sql": "CREATE TABLE {{placesTable}} ( id INT KEY, coord ARRAY[FLOAT] INDEX RTREE, label VARCHAR[32] );"
}
```

Insertar puntos vía SQL:

```
POST /users/{user_id}/databases/{db_name}/query
{
	"sql": "INSERT INTO {{placesTable}} VALUES ( id=1, coord='19.43, -99.13', label='A' );"
}

POST /users/{user_id}/databases/{db_name}/query
{
	"sql": "INSERT INTO {{placesTable}} VALUES ( id=2, coord='19.44, -99.14', label='B' );"
}
```

Consultas espaciales con endpoints dedicados (cuando existe un índice `RTREE` sobre una columna `ARRAY_FLOAT`):

- POST `/users/{user_id}/databases/{db_name}/tables/{table_name}/records/range-radius`
	- Body: `{ "column": "coord", "center": [lat, lon], "radius": 0.01 }`
	- Devuelve filas dentro de un radio (en grados aprox.).
- POST `/users/{user_id}/databases/{db_name}/tables/{table_name}/records/knn`
	- Body: `{ "column": "coord", "center": [lat, lon], "k": 3 }`
	- Devuelve los k vecinos más cercanos.

Internamente se usa una implementación propia compatible con RTREE; si el paquete opcional `Rtree` está instalado, se usa para acelerar búsquedas.

## Postman

En la carpeta `postman/` se incluyen:

- `BD2_API.postman_collection.json`: colección con carpetas para Health, Users, Databases, Tables, Records, SQL (DDL), Spatial (RTREE) e Import CSV.
- `BD2_Local.postman_environment.json`: variables `baseUrl`, `userId`, `dbName`, `tableName`.
- `sample_people.csv` y `restaurantes.csv`: archivos de ejemplo para importación.

Uso:
1. Importa el environment `BD2_Local` en Postman y ajusta variables si necesitas.
2. Importa la colección `BD2_API`.
3. Ejecuta en orden: Users → Databases → Tables/SQL → Records/Spatial según lo que quieras probar.

## Despliegue con Docker Compose

Se añadió soporte para ejecutar backend y frontend con Docker Compose.

Archivos agregados:
- `Dockerfile.api` (backend FastAPI)
- `auth-app/Dockerfile` (frontend Next.js)
- `docker-compose.yml`
- `.dockerignore` y `auth-app/.dockerignore`

Variables de entorno (crea un archivo `.env` en la raíz si deseas cambiarlas):
- `JWT_SECRET`: clave para firmar JWT.
- `CORS_ALLOWED_ORIGINS`: lista separada por comas (ej. `http://localhost:3000,http://127.0.0.1:3000`).
- `NEXT_PUBLIC_API_BASE_URL`: URL del backend que usará el frontend (por defecto `http://localhost:8000`).

Ejemplo `.env`:

```
JWT_SECRET=super-secret-key-change-me
CORS_ALLOWED_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
NEXT_PUBLIC_API_BASE_URL=http://localhost:8000
```

Cómo ejecutar (Windows PowerShell, en la raíz del repo):

```
docker compose build ; docker compose up -d
```

Servicios:
- Backend: http://localhost:8000 (Swagger: `/docs`)
- Frontend: http://localhost:3000

Logs:

```
docker compose logs -f backend
docker compose logs -f frontend
```

Detener:

```
docker compose down
```

Persistencia: La carpeta `./data` del host se monta en `/app/data` en el backend para conservar bases de datos y usuarios.

