from fastapi import APIRouter, UploadFile, File, Query, Body
from fastapi.responses import FileResponse
from typing import List, Optional
from pydantic import BaseModel, Field
import os
import pickle
import numpy as np
import mimetypes
import hashlib
import cv2

from multimedia.features_image import extract_sift_descriptors
from multimedia.features_audio import extract_mfcc_descriptors
from multimedia.codebook import load_codebook, sample_descriptors, train_codebook, save_codebook
from multimedia.bow import quantize_descriptors, compute_df, save_bow_artifacts
from multimedia.knn_sequential import search_sequential
from multimedia.inv_index import search_inverted
from multimedia.inv_index import build_inverted_index


router = APIRouter(prefix="/multimedia", tags=["multimedia"])


@router.post("/search")
async def multimedia_search(
    file: UploadFile = File(...),
    modality: str = Query(..., regex="^(image|audio)$"),
    strategy: str = Query("inverted", regex="^(sequential|inverted)$"),
    k: int = Query(10, ge=1, le=100),
):
    base_dir = os.path.join("data", "multimedia", modality)
    codebook_path = os.path.join(base_dir, "codebook.pkl")
    if not os.path.exists(codebook_path):
        return {
            "ok": False,
            "error": f"Missing codebook at {codebook_path}. Train it via /multimedia/train-codebook",
        }
    try:
        km, meta = load_codebook(codebook_path)
    except Exception as e:
        return {"ok": False, "error": f"Failed to load codebook: {e}"}
    centroids = km.cluster_centers_.astype(np.float32)

    bytes_data = await file.read()
    tmp = os.path.join(base_dir, "_tmp_input")
    os.makedirs(tmp, exist_ok=True)
    tmp_path = os.path.join(tmp, file.filename)
    with open(tmp_path, "wb") as f:
        f.write(bytes_data)

    if modality == "image":
        desc = extract_sift_descriptors(tmp_path)
    else:
        desc = extract_mfcc_descriptors(tmp_path)

    os.remove(tmp_path)

    hist = quantize_descriptors(desc, centroids)

    if strategy == "sequential":
        bow_dir = os.path.join(base_dir, "bow")
        if not os.path.exists(bow_dir):
            return {
                "ok": False,
                "error": f"Missing BoW artifacts at {bow_dir}. Build index via /multimedia/index?index_type=bow",
            }
        results = search_sequential(hist, bow_dir, top_k=k)
    else:
        index_dir = os.path.join(base_dir, "inv_index")
        if not os.path.exists(index_dir):
            return {
                "ok": False,
                "error": f"Missing inverted index at {index_dir}. Build index via /multimedia/index?index_type=inverted",
            }
        results = search_inverted(hist, index_dir, top_k=k)

    return {"ok": True, "results": [{"doc_id": doc_id, "score": score} for doc_id, score in results]}


class TrainCodebookRequest(BaseModel):
    modality: str = Field(..., pattern=r"^(image|audio)$")
    data_root: str = Field(..., description="Root folder with files to sample descriptors from")
    k: int = Field(512, ge=16, le=4096)
    per_object_cap: int = Field(500, ge=10, le=10000)
    global_cap: int = Field(200_000, ge=1000, le=2_000_000)


@router.post("/train-codebook")
async def multimedia_train_codebook(
    payload: Optional[TrainCodebookRequest] = Body(None),
    modality_q: Optional[str] = Query(None, regex="^(image|audio)$"),
    data_root_q: Optional[str] = Query(None),
    k_q: Optional[int] = Query(None, ge=16, le=4096),
    per_object_cap_q: Optional[int] = Query(None, ge=10, le=10000),
    global_cap_q: Optional[int] = Query(None, ge=1000, le=2_000_000),
):
    if payload:
        modality = payload.modality
        data_root = payload.data_root
        k = payload.k
        per_object_cap = payload.per_object_cap
        global_cap = payload.global_cap
    else:
        if modality_q is None or data_root_q is None:
            return {"ok": False, "error": "Missing modality or data_root"}
        modality = modality_q
        data_root = data_root_q
        k = k_q or 512
        per_object_cap = per_object_cap_q or 500
        global_cap = global_cap_q or 200_000
    # Collect file paths
    paths: List[str] = []
    for root, _, files in os.walk(data_root):
        for f in files:
            if modality == "image" and f.lower().endswith((".jpg", ".jpeg", ".png")):
                paths.append(os.path.join(root, f))
            elif modality == "audio" and f.lower().endswith((".mp3", ".wav", ".flac", ".ogg")):
                paths.append(os.path.join(root, f))
    if not paths:
        return {"ok": False, "error": "No files found in data_root"}

    # Extract descriptors
    descs: List[np.ndarray] = []
    if modality == "image":
        from multimedia.features_image import batch_extract_sift
        _, descs = batch_extract_sift(paths, max_keypoints=2000)
    else:
        from multimedia.features_audio import batch_extract_mfcc
        _, descs = batch_extract_mfcc(paths, sr=22050, duration=10.0, n_mfcc=20, hop_length=512)
    if not descs:
        return {"ok": False, "error": "Descriptor extraction returned empty"}

    # Sample and train
    samples = sample_descriptors(descs, per_object_cap=per_object_cap, global_cap=global_cap)
    km = train_codebook(samples, k=k, batch_size=max(1000, k*2), seed=42)
    base_dir = os.path.join("data", "multimedia", modality)
    os.makedirs(base_dir, exist_ok=True)
    dim = 128 if modality == "image" else 20
    save_codebook(km, os.path.join(base_dir, "codebook.pkl"), modality=modality, dim=dim)
    return {"ok": True, "modality": modality, "k": k, "paths": len(paths)}


class BuildIndexRequest(BaseModel):
    modality: str = Field(..., pattern=r"^(image|audio)$")
    data_root: str = Field(..., description="Root folder with files to index")
    index_type: str = Field("inverted", pattern=r"^(bow|inverted)$")


@router.post("/index")
async def multimedia_build_index(
    payload: Optional[BuildIndexRequest] = Body(None),
    modality_q: Optional[str] = Query(None, regex="^(image|audio)$"),
    data_root_q: Optional[str] = Query(None),
    index_type_q: Optional[str] = Query(None, regex="^(bow|inverted)$"),
):
    if payload:
        modality = payload.modality
        data_root = payload.data_root
        index_type = payload.index_type
    else:
        if modality_q is None or data_root_q is None:
            return {"ok": False, "error": "Missing modality or data_root"}
        modality = modality_q
        data_root = data_root_q
        index_type = index_type_q or "inverted"
    base_dir = os.path.join("data", "multimedia", modality)
    codebook_path = os.path.join(base_dir, "codebook.pkl")
    km, meta = load_codebook(codebook_path)
    centroids = km.cluster_centers_.astype(np.float32)

    # Collect files and extract descriptors
    paths: List[str] = []
    for root, _, files in os.walk(data_root):
        for f in files:
            if modality == "image" and f.lower().endswith((".jpg", ".jpeg", ".png")):
                paths.append(os.path.join(root, f))
            elif modality == "audio" and f.lower().endswith((".mp3", ".wav", ".flac", ".ogg")):
                paths.append(os.path.join(root, f))
    if not paths:
        return {"ok": False, "error": "No files found in data_root"}

    if modality == "image":
        from multimedia.features_image import batch_extract_sift
        doc_ids, descs = batch_extract_sift(paths, max_keypoints=2000)
    else:
        from multimedia.features_audio import batch_extract_mfcc
        doc_ids, descs = batch_extract_mfcc(paths, sr=22050, duration=10.0, n_mfcc=20, hop_length=512)
    if not descs:
        return {"ok": False, "error": "Descriptor extraction returned empty"}

    # Quantize
    hists = [quantize_descriptors(d, centroids) for d in descs]

    if index_type == "bow":
        df = compute_df(hists)
        out_dir = os.path.join(base_dir, "bow")
        save_bow_artifacts(out_dir, hists, doc_ids, df)
    else:
        out_dir = os.path.join(base_dir, "inv_index")
        build_inverted_index(doc_ids, hists, out_dir)

    return {"ok": True, "modality": modality, "index_type": index_type, "count": len(doc_ids)}


@router.get("/status")
async def multimedia_status(modality: Optional[str] = Query(None, regex="^(image|audio)$")):
    def check(m: str):
        base_dir = os.path.join("data", "multimedia", m)
        codebook = os.path.exists(os.path.join(base_dir, "codebook.pkl"))
        bow = os.path.exists(os.path.join(base_dir, "bow"))
        inv = os.path.exists(os.path.join(base_dir, "inv_index"))
        return {"codebook": codebook, "bow": bow, "inverted": inv}

    if modality:
        return {modality: check(modality)}
    else:
        return {"image": check("image"), "audio": check("audio")}


def _thumb_path(modality: str, doc_id: str) -> str:
    base_dir = os.path.join("data", "multimedia", modality)
    out_dir = os.path.join(base_dir, "thumbnails")
    os.makedirs(out_dir, exist_ok=True)
    name = hashlib.sha1(doc_id.encode("utf-8")).hexdigest() + ".jpg"
    return os.path.join(out_dir, name)


@router.get("/thumbnail")
async def multimedia_thumbnail(modality: str = Query(..., regex="^(image)$"), doc_id: str = Query(...)):
    # Generate or return cached thumbnail for an image doc_id
    thumb = _thumb_path(modality, doc_id)
    if not os.path.exists(thumb):
        # Basic safety: only allow existing files
        if not os.path.exists(doc_id):
            return {"ok": False, "error": "File not found"}
        img = cv2.imread(doc_id)
        if img is None:
            return {"ok": False, "error": "Failed to read image"}
        h, w = img.shape[:2]
        target = 256
        if h > w:
            new_h = target
            new_w = max(1, int(w * (target / h)))
        else:
            new_w = target
            new_h = max(1, int(h * (target / w)))
        resized = cv2.resize(img, (new_w, new_h), interpolation=cv2.INTER_AREA)
        # Pad to square
        canvas = np.zeros((target, target, 3), dtype=np.uint8)
        y0 = (target - new_h) // 2
        x0 = (target - new_w) // 2
        canvas[y0:y0+new_h, x0:x0+new_w] = resized
        cv2.imwrite(thumb, canvas)
    return FileResponse(thumb, media_type="image/jpeg")


@router.get("/preview")
async def multimedia_preview(modality: str = Query(..., regex="^(image|audio)$"), doc_id: str = Query(...)):
    # Serve original file (useful for audio or full images)
    if not os.path.exists(doc_id):
        return {"ok": False, "error": "File not found"}
    mime = mimetypes.guess_type(doc_id)[0] or "application/octet-stream"
    return FileResponse(doc_id, media_type=mime)
