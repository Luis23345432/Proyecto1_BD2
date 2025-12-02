"""Módulo para la extracción de características de audio.

Implementa la extracción de descriptores MFCC (Mel-Frequency Cepstral Coefficients)
desde archivos de audio utilizando librosa. Los MFCCs son características ampliamente
utilizadas en tareas de recuperación y clasificación de audio.
"""

import logging
from typing import List, Tuple

import numpy as np
import librosa


logger = logging.getLogger(__name__)


def extract_mfcc_descriptors(audio_path: str, sr: int = 22050, duration: float = 10.0, n_mfcc: int = 20, hop_length: int = 512) -> np.ndarray:
    """Extrae descriptores MFCC de un archivo de audio.
    
    Args:
        audio_path: Ruta al archivo de audio
        sr: Frecuencia de muestreo objetivo (Hz)
        duration: Duración máxima a procesar (segundos)
        n_mfcc: Número de coeficientes MFCC a extraer
        hop_length: Longitud del salto entre ventanas
        
    Returns:
        Matriz de descriptores MFCC (n_frames, n_mfcc). 
        Retorna matriz vacía si ocurre un error.
    """
    try:
        y, _sr = librosa.load(audio_path, sr=sr, mono=True, duration=duration)
    except Exception as e:
        logger.warning("Failed to read audio: %s (%s)", audio_path, e)
        return np.empty((0, n_mfcc), dtype=np.float32)

    if y is None or y.size == 0:
        return np.empty((0, n_mfcc), dtype=np.float32)

    mfcc = librosa.feature.mfcc(y=y, sr=sr, n_mfcc=n_mfcc, hop_length=hop_length)
    mfcc = mfcc.T.astype(np.float32)
    return mfcc


def batch_extract_mfcc(paths: List[str], sr: int = 22050, duration: float = 10.0, n_mfcc: int = 20, hop_length: int = 512) -> Tuple[List[str], List[np.ndarray]]:
    """Extrae descriptores MFCC de múltiples archivos de audio.
    
    Args:
        paths: Lista de rutas a archivos de audio
        sr: Frecuencia de muestreo objetivo (Hz)
        duration: Duración máxima a procesar (segundos)
        n_mfcc: Número de coeficientes MFCC
        hop_length: Longitud del salto entre ventanas
        
    Returns:
        Tupla (ids, descriptores) con las rutas válidas y sus descriptores
    """
    ids = []
    descs = []
    for p in paths:
        d = extract_mfcc_descriptors(p, sr=sr, duration=duration, n_mfcc=n_mfcc, hop_length=hop_length)
        if d.shape[0] > 0:
            ids.append(p)
            descs.append(d)
    return ids, descs
