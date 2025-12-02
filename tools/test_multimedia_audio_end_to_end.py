import numpy as np
from multimedia.features_audio import extract_mfcc_descriptors
from multimedia.codebook import sample_descriptors, train_codebook
from multimedia.bow import quantize_descriptors, compute_df
from multimedia.inv_index import build_inverted_index, search_inverted


def test_audio_end_to_end_synthetic(tmp_path):
    # Synthetic MFCC-like data: 4 clips, 20-d frames
    rng = np.random.default_rng(7)
    descs = [rng.random((100 + i * 20, 20), dtype=np.float32) for i in range(4)]
    doc_ids = [f"aud_{i}" for i in range(4)]

    samples = sample_descriptors(descs, per_object_cap=80, global_cap=500)
    km = train_codebook(samples, k=32, batch_size=64, seed=7)
    centroids = km.cluster_centers_.astype(np.float32)

    hists = [quantize_descriptors(d, centroids) for d in descs]
    df = compute_df(hists)

    inv_dir = tmp_path / "inv_audio"
    build_inverted_index(doc_ids, hists, str(inv_dir))

    q = hists[0]
    res = search_inverted(q, str(inv_dir), top_k=3)
    assert len(res) == 3
    # Top-1 should be the same doc id or among first two (due to randomness) but must include q's doc id in top-3
    assert doc_ids[0] in [d for d, _ in res]
