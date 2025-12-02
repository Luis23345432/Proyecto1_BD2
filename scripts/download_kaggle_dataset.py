import argparse
import os
import shutil
from pathlib import Path
from typing import Iterable

import kagglehub


def iter_image_files(root: Path) -> Iterable[Path]:
    exts = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            yield p


def mirror_images(src_root: Path, dst_root: Path, limit: int | None = None) -> int:
    dst_root.mkdir(parents=True, exist_ok=True)
    count = 0
    for img in iter_image_files(src_root):
        rel = img.relative_to(src_root)
        out_path = dst_root / rel
        out_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(img, out_path)
        count += 1
        if limit is not None and count >= limit:
            break
    return count


def main():
    parser = argparse.ArgumentParser(description="Descarga datasets de Kaggle con kagglehub y opcionalmente copia imágenes a datasets/")
    parser.add_argument("--dataset", default="paramaggarwal/fashion-product-images-dataset", help="Slug del dataset en Kaggle (owner/name)")
    parser.add_argument("--out-dir", default="datasets", help="Directorio destino dentro del repo para espejar/copiar")
    parser.add_argument("--mirror", action="store_true", help="Si se especifica, copia las imágenes al directorio del repo")
    parser.add_argument("--limit", type=int, default=None, help="Límite de imágenes a copiar (solo con --mirror)")
    args = parser.parse_args()

    print("Descargando dataset con kagglehub...")
    cache_path = Path(kagglehub.dataset_download(args.dataset))
    print(f"Ruta en caché local: {cache_path}")

    if args.mirror:
        slug_name = args.dataset.split("/")[-1]
        dst = Path(args.out_dir) / slug_name
        print(f"Copiando imágenes a: {dst} (limit={args.limit})")
        copied = mirror_images(cache_path, dst, limit=args.limit)
        print(f"Imágenes copiadas: {copied}")
        # Tip: preparar muestras
        samples_dir = Path("data/samples/images")
        samples_dir.mkdir(parents=True, exist_ok=True)
        # Copiar hasta 50 primeras si hay
        copied_samples = 0
        for img in iter_image_files(dst):
            out = samples_dir / img.name
            shutil.copy2(img, out)
            copied_samples += 1
            if copied_samples >= 50:
                break
        print(f"Muestras en {samples_dir}: {copied_samples}")

    print("Listo.")


if __name__ == "__main__":
    main()
