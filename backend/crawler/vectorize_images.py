import os

import numpy as np
import pandas as pd
from PIL import Image
from tqdm import tqdm

INPUT_PATH = "model/dataset/dataset_crawl_070526_with_images.csv"
OUTPUT_PATH = "model/dataset/dataset_crawl_070526_with_vectors.csv"
NPZ_PATH = "model/dataset/image_vectors.npz"
IMG_SIZE = 224


def load_and_vectorize(img_path: str) -> np.ndarray | None:
    try:
        img = Image.open(img_path).convert("RGB")
        img = img.resize((IMG_SIZE, IMG_SIZE), Image.Resampling.LANCZOS)
        arr = np.asarray(img, dtype=np.float32) / 255.0
        return arr.flatten()
    except Exception:
        return None


def main():
    df: pd.DataFrame = pd.read_csv(INPUT_PATH, low_memory=False)
    df["Webpage_id"] = df["Webpage_id"].astype(str)

    success_mask = df["Status"] == "success"
    total_success = int(success_mask.sum())
    print(f"Total gambar sukses: {total_success}")

    vectors: list[np.ndarray] = []
    vector_indices: dict[str, int] = {}
    errors = 0

    subset = df[success_mask].copy()
    for idx, row in tqdm(
        subset.iterrows(), total=len(subset), desc="Vectorize", unit="img"
    ):
        pid = str(row["Webpage_id"])
        path = str(row.get("Screenshot_Path", "") or "")
        if not path or path == "nan" or not os.path.isfile(path):
            errors += 1
            continue

        vec = load_and_vectorize(path)
        if vec is None:
            errors += 1
            continue

        vector_indices[pid] = len(vectors)
        vectors.append(vec)

    if not vectors:
        print("Tidak ada vektor yang dihasilkan.")
        return

    arr = np.stack(vectors, axis=0)
    np.savez_compressed(NPZ_PATH, vectors=arr)
    print(f"Tersimpan: {NPZ_PATH} ({arr.shape})")

    df["Vector_Index"] = df["Webpage_id"].map(lambda pid: vector_indices.get(pid, -1))

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"Tersimpan: {OUTPUT_PATH}")

    print(f"\nVektor: {len(vectors)}/{total_success}")
    print(f"  Shape: {arr.shape}")
    print(f"  Error: {errors}")


if __name__ == "__main__":
    main()
