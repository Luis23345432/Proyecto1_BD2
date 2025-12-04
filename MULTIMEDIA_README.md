# Multimedia Indexing (Imágenes + Audio)

Este documento resume la indexación de descriptores locales multimedia implementada en este proyecto, cómo ejecutar las pruebas y cómo usar el frontend para realizar búsquedas de similitud.

## Overview
- Features:
  - Images: SIFT local descriptors (`multimedia/features_image.py`).
  - Audio: MFCC frame descriptors (`multimedia/features_audio.py`).
- Codebook (Visual/Acoustic Dictionary):
  - Trained via `MiniBatchKMeans` with configurable `k` (`multimedia/codebook.py`).
  - Saved/loaded as pickle with metadata.
- Bag of Words + TF‑IDF:
  - Quantization of local descriptors to nearest codewords, histogram per object (`multimedia/bow.py`).
  - TF‑IDF weighting with L2 normalization for cosine similarity.
- KNN Sequential:
  - Computes cosine similarity against stored TF‑IDF histograms; maintains Top‑K via heap (`multimedia/knn_sequential.py`).
- Inverted Index:
  - Codeword → posting lists of documents with TF‑IDF weights; query accumulates scores for active codewords (`multimedia/inv_index.py`).
- API Endpoint:
  - `POST /multimedia/search?modality={image|audio}&strategy={sequential|inverted}&k={K}` accepts an uploaded file and returns Top‑K matches (`api/multimedia.py`).

Artifacts are expected under `data/multimedia/{image|audio}` for trained codebooks and indexes if you add ingest/train/index endpoints later.

## What’s Implemented Now
- End-to-end pipeline pieces for features, codebook training, BoW/TF‑IDF, sequential and inverted search.
- A frontend component to run multimedia searches by uploading a file.
- Tests that validate the pipeline on synthetic data and optionally on local samples.

## Running Tests
Ensure dependencies are installed. From the repo root:

```powershell
python -m pip install -r requirements.txt
# Run image pipeline tests
python -m pytest tools/test_multimedia_image.py -q
# Run audio pipeline tests
python -m pytest tools/test_multimedia_audio.py -q
```

Notes:
- `tools/test_multimedia_image.py` attempts SIFT extraction on `data/samples/images` if present; otherwise runs synthetic end‑to‑end.
- `tools/test_multimedia_audio.py` tries to read one audio file under `datasets/fma`; otherwise runs synthetic MFCC/codebook/quantization.

## Using the Frontend
Start the API (e.g., `uvicorn` or Docker Compose) and then run the Next.js app:

```powershell
# API example (adjust to your setup)
python -m pip install -r requirements.txt
# e.g., uvicorn api.app:app --reload --port 8000 (if applicable)

# Frontend
cd auth-app
pnpm install
pnpm dev
```

Open the DBMS page in the app. The page includes:
- Text Query area: Execute SQL-like queries against your textual DBMS.
- Multimedia Search card: Upload an image or audio file, choose modality, strategy (inverted or sequential), and Top‑K; see results with scores and execution time.

## Roadmap / Next Steps
- Add endpoints for ingestion (`/multimedia/ingest`), codebook training (`/multimedia/train-codebook`), index build (`/multimedia/index`), and status (`/multimedia/status`).
- Persist multimedia metadata (id, type, file_path, dimensions/duration, tags) to integrate richer result displays in the frontend.
- Optional: Add deep features (Inception v3/ResNet50) or FAISS for faster quantization.

## Files
- `multimedia/features_image.py`: SIFT descriptor extraction.
- `multimedia/features_audio.py`: MFCC descriptor extraction.
- `multimedia/codebook.py`: Sampling, training, save/load codebook.
- `multimedia/bow.py`: Quantization, DF/TF‑IDF, save/load BoW artifacts.
- `multimedia/knn_sequential.py`: TF‑IDF normalization and cosine Top‑K.
- `multimedia/inv_index.py`: Build/search inverted index over codewords.
- `api/multimedia.py`: FastAPI endpoint for multimedia search.
- `auth-app/components/multimedia-search.tsx`: UI component for image/audio similarity search.
# Indexación Multimedia (Imágenes + Audio)

Este módulo agrega indexación y búsqueda basada en descriptores locales para imágenes (SIFT) y audio (MFCC). Soporta dos estrategias: KNN secuencial sobre histogramas TF‑IDF y búsqueda con índice invertido de codewords para Top‑K eficiente.

## Qué está implementado
- Extracción SIFT para imágenes (`multimedia/features_image.py`).
- Extracción MFCC para audio (`multimedia/features_audio.py`).
- Entrenamiento de diccionario (codebook) con MiniBatchKMeans (`multimedia/codebook.py`).
- Cuantización BoW, DF/TF‑IDF, normalización y persistencia (`multimedia/bow.py`).
- KNN secuencial (coseno + heap Top‑K) (`multimedia/knn_sequential.py`).
- Índice invertido por codewords y búsqueda (`multimedia/inv_index.py`).
- Endpoint FastAPI `/multimedia/search` (subir archivo, elegir modalidad y estrategia).
- Dependencias añadidas a `requirements.txt` (OpenCV, scikit‑learn, librosa, etc.).

## Estructura de directorios
- `data/multimedia/image/` y `data/multimedia/audio/` guardan artefactos:
  - `codebook.pkl`: centroides del k‑means y metadatos.
  - `bow/`: histogramas por documento (`bow_*.npz`), `doc_ids.pkl`, `df.pkl`.
  - `inv_index/`: índice invertido (`cw_*.pkl`, `doc_ids.pkl`, `idf.pkl`, `term_to_block.pkl`).

## Inicio rápido

1. Instalar dependencias (recomendado Docker; local también funciona):
```
pip install -r requirements.txt
```

2. Dataset pequeño de prueba:
- Imágenes: `data/samples/images/` con JPG/PNG.
- Audios: `data/samples/audio/` con WAV/MP3.

3. Entrenar diccionario (codebook) en un REPL/script de Python:
```
from multimedia.features_image import batch_extract_sift
from multimedia.features_audio import batch_extract_mfcc
from multimedia.codebook import sample_descriptors, train_codebook, save_codebook
import os

# Imágenes
imgs = [os.path.join('data','samples','images',f) for f in os.listdir('data/samples/images')]
ids_i, descs_i = batch_extract_sift(imgs)
samples_i = sample_descriptors(descs_i, per_object_cap=2000, global_cap=100000)
km_i = train_codebook(samples_i, k=512)
os.makedirs('data/multimedia/image', exist_ok=True)
save_codebook(km_i, 'data/multimedia/image/codebook.pkl', modality='image', dim=samples_i.shape[1])

# Audio (opcional)
auds = [os.path.join('data','samples','audio',f) for f in os.listdir('data/samples/audio')]
ids_a, descs_a = batch_extract_mfcc(auds)
samples_a = sample_descriptors(descs_a, per_object_cap=5000, global_cap=100000)
km_a = train_codebook(samples_a, k=512)
os.makedirs('data/multimedia/audio', exist_ok=True)
save_codebook(km_a, 'data/multimedia/audio/codebook.pkl', modality='audio', dim=samples_a.shape[1])
```

4. Construir histogramas BoW e índice invertido (imágenes como ejemplo):
```
from multimedia.bow import quantize_descriptors, compute_df, save_bow_artifacts
from multimedia.codebook import load_codebook
from multimedia.features_image import batch_extract_sift
from multimedia.inv_index import build_inverted_index
import numpy as np, os

base = 'data/multimedia/image'
km, meta = load_codebook(os.path.join(base,'codebook.pkl'))
centroids = km.cluster_centers_.astype('float32')
imgs = [os.path.join('data','samples','images',f) for f in os.listdir('data/samples/images')]
ids, descs = batch_extract_sift(imgs)
hists = [quantize_descriptors(d, centroids) for d in descs]
df = compute_df(hists)
os.makedirs(os.path.join(base,'bow'), exist_ok=True)
save_bow_artifacts(os.path.join(base,'bow'), hists, ids, df)
os.makedirs(os.path.join(base,'inv_index'), exist_ok=True)
build_inverted_index(ids, hists, os.path.join(base,'inv_index'))
```

5. Levantar la API y probar búsqueda:
```
python -m uvicorn api.app:create_app --factory --host 0.0.0.0 --port 8000
```

Probar con Postman o curl (subiendo un archivo):
```
curl -X POST "http://localhost:8000/multimedia/search?modality=image&strategy=inverted&k=5" -F "file=@data/samples/images/example.jpg"
```

 La respuesta incluye `doc_id` (ruta original) y `score` de similitud.

## Actualizaciones y uso completo (2025-12)

Esta versión añade endpoints para orquestar el pipeline y previews optimizados en el frontend.

### Endpoints API
- `POST /multimedia/train-codebook` (JSON o query): entrena k‑means y guarda `codebook.pkl`.
  - Ejemplo JSON:
    ```json
    {"modality":"image","data_root":"C:/.../Proyecto1_BD2/datasets/fashion-product-images-dataset/fashion-dataset","k":256,"per_object_cap":500,"global_cap":200000}
    ```
- `POST /multimedia/index` (JSON o query): construye `bow/` o `inv_index/`.
  - Ejemplo JSON:
    ```json
    {"modality":"image","data_root":"C:/.../Proyecto1_BD2/datasets/fashion-product-images-dataset/fashion-dataset","index_type":"inverted"}
    ```
- `GET /multimedia/status?modality=image|audio`: reporte `{ codebook, bow, inverted }`.
- `POST /multimedia/search?modality=image|audio&strategy=sequential|inverted&k=10`: subida de archivo y Top‑K.
- Previews imagen:
  - `GET /multimedia/thumbnail?modality=image&doc_id=<path>`: genera y cachea thumbnail 256×256.
  - `GET /multimedia/preview?modality=image&doc_id=<path>`: sirve el archivo original.

### Artefactos
- `data/multimedia/image/codebook.pkl`
- `data/multimedia/image/inv_index/` y/o `bow/`
- `data/multimedia/image/thumbnails/` (cache de previews)
- Análogos bajo `data/multimedia/audio/` si se construye audio.

### Pruebas (pytest)
Desde la raíz:
```powershell
python -m pytest tools/test_multimedia_image.py -q
python -m pytest tools/test_multimedia_audio.py -q
python -m pytest tools/test_multimedia_common.py -q
python -m pytest tools/test_multimedia_consistency.py -q
python -m pytest tools/test_multimedia_audio_end_to_end.py -q
```

### Frontend (Next.js)
- Variable de entorno:
```powershell
$env:NEXT_PUBLIC_API_BASE_URL="http://127.0.0.1:8000"
```
- Arranque:
```powershell
cd auth-app
npm install --legacy-peer-deps
npm run dev
```
- Uso en la tarjeta “Multimedia Search”:
  - Define `Data Root`, pulsa `Train Codebook` y `Build Index` (inverted/bow).
  - Verifica con `Check Status`.
  - Selecciona archivo de consulta, `Modality`, estrategia y `Top‑K`, luego `Search`.
  - Se presentan resultados con `score` y thumbnails (256×256 cacheados; fallback al original si la miniatura falla).

### Consejos
- En Windows, prefiera JSON para enviar `data_root` (rutas seguras). Los endpoints también aceptan query params por compatibilidad.
- La UI maneja respuestas `{ ok:false }` para evitar errores al renderizar.
- Para datasets grandes, evalúe FAISS para acelerar cuantización; este proyecto usa NumPy/SciPy + scikit‑learn.

## Notas y consejos
- Para datasets grandes, considere `faiss-cpu` para cuantización más rápida.
- `k`, límites de muestreo por objeto y tope global impactan rendimiento y precisión.
- Asegure que OpenCV pueda leer sus formatos; pueden requerirse códecs del sistema.
- En audio, prefiera WAV para decodificación consistente; MP3 puede requerir ffmpeg.
