import os

import numpy as np
import pandas as pd
from PIL import Image
from tqdm import tqdm

INPUT_PATH = "model/dataset/dataset_crawl_070526_with_images_path.csv"
OUTPUT_PATH = "model/dataset/dataset_crawl_070526_with_vectors.csv"
IMG_SIZE = 224
BATCH_SIZE = 64
VEC_DIM = IMG_SIZE * IMG_SIZE * 3


def load_and_vectorize(img_path: str) -> np.ndarray | None:
    try:
        img = Image.open(img_path).convert("RGB")
        img = img.resize((IMG_SIZE, IMG_SIZE), Image.Resampling.LANCZOS)
        arr = np.asarray(img, dtype=np.float32) / 255.0
        return arr.flatten()
    except Exception:
        return None


def main():
    df = pd.read_csv(INPUT_PATH, low_memory=False)
    df["Webpage_id"] = df["Webpage_id"].astype(str)

    success_mask = df["Status"] == "success"
    total_success = int(success_mask.sum())
    total_all = len(df)

    print(f"Total baris: {total_all}")
    print(f"Status success: {total_success}")
    print(f"Dimensi vector: {VEC_DIM}")

    subset = df[success_mask].copy()
    vec_list = [None] * len(subset)
    errors = 0

    for start in range(0, len(subset), BATCH_SIZE):
        end = min(start + BATCH_SIZE, len(subset))
        batch = subset.iloc[start:end]

        for offset, (idx, row) in enumerate(
            tqdm(
                batch.iterrows(),
                total=len(batch),
                desc=f"Batch {start // BATCH_SIZE + 1}",
                unit="img",
            )
        ):
            i = start + offset
            path = str(row.get("Screenshot_Path", ""))
            if not path or path == "nan" or not os.path.isfile(path):
                errors += 1
                continue

            vec = load_and_vectorize(path)
            if vec is None:
                errors += 1
                continue

            vec_list[i] = vec

    valid_indices = [i for i, v in enumerate(vec_list) if v is not None]
    valid_vecs = np.array([vec_list[i] for i in valid_indices])

    print(f"\nVector berhasil: {len(valid_vecs)}/{total_success}")
    print(f"  Shape: {valid_vecs.shape}")
    print(f"  Error: {errors}")

    pix_cols = [f"pix_{i}" for i in range(VEC_DIM)]
    vec_df = pd.DataFrame(
        valid_vecs, columns=pix_cols, index=subset.index[valid_indices]
    )

    for col in pix_cols:
        df[col] = np.nan
    df.update(vec_df)

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nTersimpan: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
