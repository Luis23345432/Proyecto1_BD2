import argparse
import os
import random
import shutil
from pathlib import Path


def collect_images(root: Path):
    exts = {".jpg", ".jpeg", ".png", ".bmp", ".gif", ".webp"}
    for p in root.rglob("*"):
        if p.is_file() and p.suffix.lower() in exts:
            yield p


def main():
    parser = argparse.ArgumentParser(description="Copia N imágenes aleatorias desde un dataset a data/samples/images")
    parser.add_argument("src", help="Carpeta raíz del dataset local (por ejemplo datasets/fashion-product-images-dataset)")
    parser.add_argument("--n", type=int, default=50, help="Número de imágenes a copiar")
    args = parser.parse_args()

    src = Path(args.src)
    dst = Path("data/samples/images")
    dst.mkdir(parents=True, exist_ok=True)

    imgs = list(collect_images(src))
    if not imgs:
        print("No se encontraron imágenes en la carpeta especificada")
        return

    random.seed(42)
    random.shuffle(imgs)
    take = imgs[: args.n]

    copied = 0
    for img in take:
        out = dst / img.name
        shutil.copy2(img, out)
        copied += 1

    print(f"Copiadas {copied} imágenes a {dst}")


if __name__ == "__main__":
    main()
