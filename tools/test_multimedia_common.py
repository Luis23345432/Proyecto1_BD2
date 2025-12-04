import numpy as np
from multimedia.bow import compute_tfidf
from multimedia.codebook import train_codebook, save_codebook, load_codebook


def test_tfidf_weighting_vector_norm():
    # Histograms for 3 docs, k=5
    hists = [
        np.array([3, 0, 1, 0, 0], dtype=np.float32),
        np.array([0, 2, 0, 1, 0], dtype=np.float32),
        np.array([0, 1, 0, 0, 4], dtype=np.float32),
    ]
    df = np.array([1, 2, 1, 1, 1], dtype=np.int32)
    n_docs = 3
    w0 = compute_tfidf(hists[0], df, n_docs)
    w1 = compute_tfidf(hists[1], df, n_docs)
    w2 = compute_tfidf(hists[2], df, n_docs)
    # L2 norms should be 1 (or 0 if empty); these are non-empty
    assert np.isclose(np.linalg.norm(w0), 1.0)
    assert np.isclose(np.linalg.norm(w1), 1.0)
    assert np.isclose(np.linalg.norm(w2), 1.0)


def test_codebook_save_load_roundtrip(tmp_path):
    # Train a small codebook and ensure persistence works
    samples = np.random.rand(500, 16).astype(np.float32)
    km = train_codebook(samples, k=8, batch_size=64, seed=123)
    path = tmp_path / "codebook.pkl"
    save_codebook(km, str(path), modality="image", dim=16)
    km2, meta = load_codebook(str(path))
    assert km2.n_clusters == 8
    assert meta["dim"] == 16
    # Centroids should be identical upon load
    c1 = km.cluster_centers_.astype(np.float32)
    c2 = km2.cluster_centers_.astype(np.float32)
    assert np.allclose(c1, c2)
