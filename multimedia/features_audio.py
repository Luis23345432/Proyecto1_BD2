import logging
from typing import List, Tuple

import numpy as np
import librosa


logger = logging.getLogger(__name__)


def extract_mfcc_descriptors(audio_path: str, sr: int = 22050, duration: float = 10.0, n_mfcc: int = 20, hop_length: int = 512) -> np.ndarray:
    """Extract MFCC frame descriptors from an audio file.

    Returns an array of shape (n_frames, n_mfcc) or empty array.
    """
    try:
        y, _sr = librosa.load(audio_path, sr=sr, mono=True, duration=duration)
    except Exception as e:
        logger.warning("Failed to read audio: %s (%s)", audio_path, e)
        return np.empty((0, n_mfcc), dtype=np.float32)

    if y is None or y.size == 0:
        return np.empty((0, n_mfcc), dtype=np.float32)

    mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=n_mfcc, hop_length=hop_length)
    # shape: (n_mfcc, n_frames) -> transpose to (n_frames, n_mfcc)
    mfcc = mfcc.T.astype(np.float32)
    return mfcc


def batch_extract_mfcc(paths: List[str], sr: int = 22050, duration: float = 10.0, n_mfcc: int = 20, hop_length: int = 512) -> Tuple[List[str], List[np.ndarray]]:
    ids = []
    descs = []
    for p in paths:
        d = extract_mfcc_descriptors(p, sr=sr, duration=duration, n_mfcc=n_mfcc, hop_length=hop_length)
        if d.shape[0] > 0:
            ids.append(p)
            descs.append(d)
    return ids, descs
