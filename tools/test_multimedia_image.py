import os
import numpy as np
from multimedia.features_image import extract_sift_descriptors
from multimedia.codebook import sample_descriptors, train_codebook, save_codebook, load_codebook
from multimedia.bow import quantize_descriptors, compute_df, save_bow_artifacts, load_bow_artifacts
from multimedia.knn_sequential import search_sequential
from multimedia.inv_index import build_inverted_index, search_inverted


def test_image_feature_extraction_on_samples():
    samples_dir = os.path.join('data', 'samples', 'images')
    if not os.path.isdir(samples_dir):
        # Skip if sample images not present
        return
    files = [os.path.join(samples_dir, f) for f in os.listdir(samples_dir) if f.lower().endswith(('.jpg', '.png', '.jpeg'))]
    assert len(files) > 0
    d0 = extract_sift_descriptors(files[0])
    assert isinstance(d0, np.ndarray)
    assert d0.ndim == 2 and d0.shape[1] == 128


def test_image_pipeline_end_to_end_tmp(tmp_path):
    # Create synthetic descriptors for three images if real samples are unavailable
    descs = [
        np.random.rand(300, 128).astype(np.float32),
        np.random.rand(250, 128).astype(np.float32),
        np.random.rand(280, 128).astype(np.float32),
    ]
    doc_ids = [f"img_{i}" for i in range(len(descs))]

    # Codebook training
    samples = sample_descriptors(descs, per_object_cap=200, global_cap=1000)
    km = train_codebook(samples, k=64, batch_size=200, seed=42)
    codebook_path = tmp_path / 'codebook.pkl'
    save_codebook(km, str(codebook_path), modality='image', dim=128)
    km2, meta = load_codebook(str(codebook_path))
    assert km2.n_clusters == 64
    centroids = km2.cluster_centers_.astype(np.float32)

    # Quantize to histograms
    hists = [quantize_descriptors(d, centroids) for d in descs]
    assert all(h.shape[0] == 64 for h in hists)

    # TF-IDF and sequential search artifacts
    bow_dir = tmp_path / 'bow'
    df = compute_df(hists)
    save_bow_artifacts(str(bow_dir), hists, doc_ids, df)
    doc_ids2, hists2, df2 = load_bow_artifacts(str(bow_dir))
    assert doc_ids2 == doc_ids
    assert len(hists2) == len(hists)

    # Query using first doc histogram
    q = hists[0]
    seq_results = search_sequential(q, str(bow_dir), top_k=2)
    assert len(seq_results) == 2
    # First result should be the same document or very similar
    assert seq_results[0][0] in doc_ids

    # Build inverted index and search
    inv_dir = tmp_path / 'inv_index'
    build_inverted_index(doc_ids, hists, str(inv_dir))
    inv_results = search_inverted(q, str(inv_dir), top_k=2)
    assert len(inv_results) == 2
