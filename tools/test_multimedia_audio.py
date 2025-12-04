import os
import numpy as np
from multimedia.features_audio import extract_mfcc_descriptors
from multimedia.codebook import sample_descriptors, train_codebook
from multimedia.bow import quantize_descriptors


def test_audio_feature_extraction_on_samples():
    # Try a few audio files under datasets/fma if present
    fma_dir = os.path.join('datasets', 'fma')
    candidates = []
    if os.path.isdir(fma_dir):
        for root, _, files in os.walk(fma_dir):
            for f in files:
                if f.lower().endswith(('.mp3', '.wav', '.flac', '.ogg')):
                    candidates.append(os.path.join(root, f))
                    if len(candidates) >= 1:
                        break
            if candidates:
                break
    if not candidates:
        # Skip if no audio sample available locally
        return
    d = extract_mfcc_descriptors(candidates[0], sr=22050, duration=5.0, n_mfcc=20)
    assert isinstance(d, np.ndarray)
    assert d.ndim == 2 and d.shape[1] == 20


def test_audio_codebook_and_quantization():
    # Synthetic MFCC frames for 2 audio clips
    descs = [
        np.random.rand(120, 20).astype(np.float32),
        np.random.rand(150, 20).astype(np.float32),
    ]
    samples = sample_descriptors(descs, per_object_cap=60, global_cap=200)
    km = train_codebook(samples, k=32, batch_size=64, seed=42)
    centroids = km.cluster_centers_.astype(np.float32)
    h0 = quantize_descriptors(descs[0], centroids)
    h1 = quantize_descriptors(descs[1], centroids)
    assert h0.shape == (32,)
    assert h1.shape == (32,)
