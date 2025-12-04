import numpy as np
from multimedia.bow import quantize_descriptors, compute_df
from multimedia.codebook import train_codebook
from multimedia.knn_sequential import search_sequential
from multimedia.inv_index import build_inverted_index, search_inverted


def test_sequential_vs_inverted_consistency(tmp_path):
    # Synthetic descriptors for 5 docs, dim=32; train codebook k=16
    rng = np.random.default_rng(42)
    descs = [rng.random((200 + i * 10, 32), dtype=np.float32) for i in range(5)]
    samples = np.vstack(descs)
    km = train_codebook(samples, k=16, batch_size=128, seed=42)
    centroids = km.cluster_centers_.astype(np.float32)
    doc_ids = [f"doc_{i}" for i in range(5)]

    hists = [quantize_descriptors(d, centroids) for d in descs]
    df = compute_df(hists)

    # Build artifacts for sequential
    bow_dir = tmp_path / "bow"
    from multimedia.bow import save_bow_artifacts
    save_bow_artifacts(str(bow_dir), hists, doc_ids, df)

    # Build artifacts for inverted
    inv_dir = tmp_path / "inv"
    build_inverted_index(doc_ids, hists, str(inv_dir))

    # Query: use the first doc histogram
    q = hists[0]
    res_seq = search_sequential(q, str(bow_dir), top_k=5)
    res_inv = search_inverted(q, str(inv_dir), top_k=5)

    # Compare top-1 doc and ensure both include the same doc ids set for top-5 (order may differ slightly)
    assert res_seq[0][0] == res_inv[0][0]
    assert set([d for d, _ in res_seq]) == set([d for d, _ in res_inv])
