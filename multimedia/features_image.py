import logging
from typing import List, Tuple

import cv2
import numpy as np


logger = logging.getLogger(__name__)


def extract_sift_descriptors(image_path: str, max_keypoints: int = 2000) -> np.ndarray:
    """Extract SIFT descriptors (128-d) from an image path.

    Returns an array of shape (n_keypoints, 128) or empty array if none.
    """
    img = cv2.imread(image_path, cv2.IMREAD_GRAYSCALE)
    if img is None:
        logger.warning("Failed to read image: %s", image_path)
        return np.empty((0, 128), dtype=np.float32)

    try:
        sift = cv2.SIFT_create()
    except Exception as e:
        logger.error("SIFT_create failed: %s", e)
        return np.empty((0, 128), dtype=np.float32)

    keypoints = sift.detect(img, None)
    if not keypoints:
        return np.empty((0, 128), dtype=np.float32)

    # Keep strongest keypoints
    keypoints = sorted(keypoints, key=lambda k: -k.response)[:max_keypoints]
    keypoints, descriptors = sift.compute(img, keypoints)
    if descriptors is None:
        return np.empty((0, 128), dtype=np.float32)
    d = descriptors.astype(np.float32)
    # RootSIFT: L1 normalize each descriptor then take element-wise sqrt
    # Avoid division by zero by adding small epsilon
    eps = 1e-12
    l1 = np.maximum(np.sum(np.abs(d), axis=1, keepdims=True), eps)
    d = d / l1
    d = np.sqrt(np.maximum(d, 0.0))
    return d


def batch_extract_sift(paths: List[str], max_keypoints: int = 2000) -> Tuple[List[str], List[np.ndarray]]:
    ids = []
    descs = []
    for p in paths:
        d = extract_sift_descriptors(p, max_keypoints=max_keypoints)
        if d.shape[0] > 0:
            ids.append(p)
            descs.append(d)
    return ids, descs
