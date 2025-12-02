# Proyecto 1 – Base de Datos 2 (CS2702)

Informe técnico conciso y guía de uso del sistema multimodal de recuperación por contenido: texto (full‑text) e imágenes/audio (BoW/Índice invertido), con backend FastAPI y frontend Next.js.

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

## Introducción
- Dominio de datos: texto estructurado (tablas con columnas de lenguaje natural) e imágenes/audio (descriptores locales). El objetivo es recuperar por contenido: en texto, documentos similares a una consulta; en multimedia, objetos visuales/sonoros similares al ejemplo.
- Justificación: una base multimodal combina índice invertido full‑text con técnicas de descriptores (SIFT/RootSIFT, MFCC) y BoW para soportar consultas eficientes sin cargar toda la colección en memoria, aprovechando índices de disco (SPIMI para texto; BoW + índice invertido para multimedia).

## Backend – Índice Invertido para Texto
- Construcción en memoria secundaria (SPIMI):
	- Bloques: se tokeniza/filtra cada documento y se generan archivos JSON por bloque con postings `t -> [[docid, tf], ...]`.
	- Merge multi‑bloque: se fusionan vía heap y se generan archivos por término bajo `index_dir/terms/<term>.json`.
	- Meta y normas de documento: se computa TF‑IDF y norma L2 por documento; para colecciones grandes, se fragmenta en shards de `doc_norms/`.
- Consulta eficiente (Coseno Top‑K):
	- Para la consulta se leen solo los postings de términos consultados, se acumula el producto punto TF‑IDF y se divide por las normas; se retorna Top‑K ordenado por similitud.
- PostgreSQL (referencia):
	- PostgreSQL implementa índices invertidos mediante GIN/TSVector. Nuestro enfoque replica el pipeline: tokenización, stopwords, stemming, TF‑IDF y cosine, pero en un diseño de archivos JSON accesibles sin carga total a RAM.
- Archivos clave: `indexes/spimi.py`, `indexes/inverted_index.py`, `api/spimi.py`, `parser/*`.

## Backend – Índice Invertido para Descriptores Locales
- Bag of Visual/Acoustic Words:
	- Extracción: imágenes con SIFT normalizado (RootSIFT) y audio con MFCC; ver `multimedia/features_image.py`, `multimedia/features_audio.py`.
	- Codebook: MiniBatchKMeans (p. ej., k=512–1024) entrenado sobre muestras; ver `multimedia/codebook.py`.
	- BoW: cuantización con soft‑assignment (Top‑3 centroides con pesos gaussianos), TF‑IDF con TF sublineal y normalización L2; ver `multimedia/bow.py`.
- Indexación:
	- Secuencial KNN: se normalizan histogramas y se calcula la similitud coseno contra todos; útil para colecciones medianas.
	- Índice invertido multimedia: postings por codeword con pesos TF‑IDF normalizados; búsqueda rápida cargando solo codewords activos; ver `multimedia/inv_index.py`.
- Maldición de la dimensionalidad:
	- Impacto: histogramas de alta dimensión pueden dificultar separación.
	- Mitigación: RootSIFT, TF sublineal, soft‑assignment, aumentar k del codebook, y (opcional) PCA/whitening previo al k‑means.

## Frontend (GUI) y Mini‑manual
- App Next.js en `auth-app/` con dos paneles:
	- DBMS: ejecutar consultas SQL‑like; campo Top‑K editable y builder de índices SPIMI multi‑columna.
	- Multimedia: entrenar codebook, construir BoW o índice invertido, consultar por imagen/audio, elegir estrategia (Sequential KNN o Inverted Index), y ver resultados con puntajes y tiempos.
- Indicadores de estado:
	- Muestra `codebook/bow/inverted` para la modalidad actual (verde: listo, rojo: faltante).
- Mini‑manual de uso:
	1) Establecer `Data Root`.
	2) “Train Codebook” con k adecuado.
	3) “Build BoW” o “Build Index (inverted)”.
	4) “Check Status” hasta ver todos verdes.
	5) Seleccionar archivo y “Search”; ajustar Top‑K.
	6) Texto: usar `@@` y/o `LIMIT K`.

Capturas de referencia (en el proyecto):
- Postman collections en `postman/` para SPIMI y multimedia.
- Panel Multimedia con badges “Ready” (codebook/bow/inverted) y resultados con tiempos.

## Experimentación
- Script: `benchmarks/run_experiments.py` genera `benchmarks/report.json` (texto SPIMI y multimedia KNN secuencial vs invertido).
- Medición: el frontend reporta `Execution time (ms)` por búsqueda; el script resume latencias y cuenta de resultados.
- Análisis: comparar rendimiento y escalabilidad; para multimedia, el índice invertido suele superar al KNN secuencial en consultas BoW dispersas.
- Herramientas de comparación externas (opcional): `pgVector` (PostgreSQL) y `Faiss` (ANN CPU/GPU).

<!-- Conclusiones removidas según solicitud -->

## Ejemplos de uso rápidos
- Texto SPIMI:
```sql
SELECT title, artist, lyric
FROM Audio
WHERE lyric @@ 'amor en tiempos de guerra'
LIMIT 10;
```
- Multimedia BoW secuencial:
```python
# Entrena codebook, cuantiza y guarda BoW; luego consulta
from multimedia.bow import quantize_descriptors, compute_df, save_bow_artifacts
from multimedia.knn_sequential import search_sequential
results = search_sequential(query_hist, "data/multimedia/image/bow", top_k=5)
```
- Multimedia invertido:
```python
from multimedia.inv_index import build_inverted_index, search_inverted
build_inverted_index(doc_ids, hists, "data/multimedia/image/inv")
search_inverted(query_hist, "data/multimedia/image/inv", top_k=5)
```

## Cómo correr
- Backend (PowerShell):
```
python -m pip install --upgrade pip wheel
pip install -r requirements.txt
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```
- Frontend (PowerShell):
```
$env:NEXT_PUBLIC_API_BASE_URL="http://127.0.0.1:8000"
cd auth-app
npm install
npm run dev
```
- Tests (PowerShell):
```
python -m pytest -q
```

### Pruebas (pytest)
- Configuración de descubrimiento en `pytest.ini` para evitar problemas con comodines:
	- `testpaths = tools`
	- Patrones de archivos/funciones ajustados.
- Comandos útiles:
	- Ejecutar todo: `python -m pytest -q`
	- Ejecutar solo multimedia: `python -m pytest -q tools/test_multimedia_image.py`

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

## Nuevas características implementadas (Full-Text / SPIMI / Stemming)

Añadido soporte completo para búsqueda full-text e índice invertido en memoria secundaria, incluyendo:

- `FULLTEXT` / InvertedIndex:
	- Nuevo `IndexType.FULLTEXT` y la clase `indexes.InvertedIndex` para indexado por términos.
	- Tokenización robusta: normalización Unicode (quita diacríticos), lowercase, filtrado de stopwords y tokens cortos.
	- Soporte opcional de stemming usando `snowballstemmer` (si está instalado). Si no está, hay un fallback ligero.
	- Persistencia de índices invertidos en JSON con metadata `_meta: {"do_stem": true|false}` para compatibilidad.

- Integración en `storage/table.py`:
	- Las columnas con índice `FULLTEXT` crean un `InvertedIndex(do_stem=True)` por defecto.
	- El índice se reconstruye en `build_indexes_from_datafile()` y se actualiza en inserciones (insert/incremental y bulk).

- SPIMI (Single-Pass In-Memory Indexing) para índices en memoria secundaria:
	- Módulo `indexes/spimi.py` con:
		- `build_spimi_blocks(...)`: genera bloques SPIMI (term -> postings) desde documentos.
		- `merge_blocks(...)`: fusiona bloques y escribe archivos por término (`index_dir/terms/<term>.json`) y `meta.json` con `N` y `doc_norms`.
		- `search_topk(...)`: busca top-k por similitud coseno leyendo únicamente las postings de los términos de la consulta.
	- Los archivos por término usan nombres seguros (URL-quoted) para soportar caracteres/acento.
	- TF-IDF usado: tf = 1 + log(tf), idf = log((N+1)/df) (suavizado) y norma del documento precomputada para coseno.
	- Merge multi-way con Priority Queue (heap): al fusionar bloques SPIMI se utiliza una cola de prioridad (heap) para realizar un merge multi-entrada eficiente por clave (término), reduciendo memoria intermedia y permitiendo un ordenamiento estable de términos.

- Endpoints API añadidos:
	- `POST /users/{user}/databases/{db}/tables/{table}/spimi/build?column=description` → construye SPIMI desde los datos de la tabla.
	- `GET  /users/{user}/databases/{db}/tables/{table}/spimi/search?query=...&k=...` → devuelve top-k resultados ordenados por `score` (coseno) y el `record` completo.
	- Tras `POST /.../load-csv?bulk=true` la API ahora **construye automáticamente** índices SPIMI para columnas `FULLTEXT` y crea una ruta canónica `spimi_index/` en el directorio de la tabla (para compatibilidad con los endpoints).

- Colección Postman para pruebas:
	- Se añadió `postman/SPIMI_Test.postman_collection.json` con un flujo completo: register → login (captura token) → crear DB → crear tabla (schema con `FULLTEXT`) → subir CSV (bulk) → build SPIMI (automático) → SPIMI search.

Cómo probar rápidamente (resumen)
1. Instala deps y arranca el servidor:
```powershell
python -m pip install -r requirements.txt
uvicorn api.app:app --reload --host 127.0.0.1 --port 8000
```
2. En Postman importa `postman/SPIMI_Test.postman_collection.json` y ejecuta los pasos en orden.
3. Tras subir el CSV en modo `bulk=true` el backend reconstruirá índices y generará `spimi_index/` en la carpeta de la tabla.
4. Llama a `/spimi/search?query=...&k=...` para obtener resultados con `score` y `record`.

Notas y consideraciones
- Compatibilidad: la búsqueda incluye fallbacks (normalized / non-normalized, stemmed / unstemmed) para empatar índices antiguos sin necesidad de reindexar inmediatamente; aun así, para obtener resultados consistentes con stemming es recomendable reconstruir el índice (`bulk` o `spimi/build`).
- Escalabilidad: la implementación SPIMI actual escribe bloques en JSON y los fusiona en memoria; para colecciones grandes se recomienda adaptar `merge_blocks` para hacer un merge en streaming controlando buffers (MergeBlocks con B buffers). Puedo añadir esa versión si la necesitas para el informe.
- Logs y debugging: durante `load-csv?bulk=true` y `spimi/build` se imprimen trazas sobre el número de documentos y estado del merge; revisa la consola del servidor para confirmar `meta.json` y archivos de términos.

Si quieres, puedo:
- añadir un endpoint que haga `export_table_to_spimi_docs` + `spimi/build` en una sola llamada; o
- mejorar `merge_blocks` para respetar memoria limitada y documentar MergeBlocks con gráficos para tu informe.

*** Fin de las notas añadidas ***

## Resumen de Implementaciones Clave
- Full‑Text (Texto):
	- SPIMI por bloques + merge multi‑way; archivos por término y `meta.json` (N, doc_norms sharded).
	- Búsqueda Top‑K por coseno leyendo postings de términos de la consulta.
	- Stemming opcional (snowballstemmer) y normalización Unicode; stopwords ampliadas.
- Multimedia (Imágenes/Audio):
	- RootSIFT (post‑SIFT) y MFCC; codebook con MiniBatchKMeans.
	- BoW con soft‑assignment (Top‑3, Gaussian weights), TF sublineal y normalización L2.
	- KNN secuencial y búsqueda indexada con postings por codeword e IDF; chequeo de dimensiones y manejo de errores en API.
- Frontend:
	- Panel Multimedia con “Train Codebook”, “Build BoW”, “Build Inverted”, “Check Status”, Top‑K editable y persistencia de Data Root.
	- Panel DBMS con editor SQL, builder SPIMI y mejoras de usabilidad.
- CSV Import:
	- Detección de dialecto (csv.Sniffer), manejo de nulls y coerción segura (INT/FLOAT), ignorar columnas extra.

## Conclusión
El sistema integra un backend con índices invertidos para texto y multimedia, y una GUI práctica para entrenar, indexar y consultar. Las decisiones de diseño (SPIMI, TF‑IDF, RootSIFT, soft‑assignment) equilibran precisión y rendimiento; la API y el frontend incluyen validaciones y estados para uso fluido. Para escalas mayores, se sugiere comparar con `pgVector`/`Faiss` y considerar ANN.
