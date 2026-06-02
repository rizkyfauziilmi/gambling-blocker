import os

import pandas as pd

DATA_PATH = "model/dataset/dataset_crawl_070526.csv"
SCREENSHOT_DIR = "model/screenshots"
OUTPUT_PATH = "model/dataset/dataset_crawl_070526_with_images_path.csv"


def build_image_map():
    image_map = {}
    for tag_dir in ("gambling", "non-gambling"):
        dirpath = os.path.join(SCREENSHOT_DIR, tag_dir)
        if not os.path.isdir(dirpath):
            continue
        for fname in os.listdir(dirpath):
            stem, ext = os.path.splitext(fname)
            if ext == ".png":
                image_map[stem] = {
                    "screenshot_path": os.path.join(dirpath, fname),
                    "status": "success",
                }
            elif ext in {".blocked", ".blank", ".error"}:
                image_map.setdefault(stem, {}).update(
                    {
                        "screenshot_path": "",
                        "status": ext.lstrip("."),
                    }
                )
    return image_map


def main():
    df = pd.read_csv(DATA_PATH)
    df["Webpage_id"] = df["Webpage_id"].astype(str)

    image_map = build_image_map()
    df["Screenshot_Path"] = df["Webpage_id"].map(
        lambda pid: image_map.get(pid, {}).get("screenshot_path", "")
    )
    df["Status"] = df["Webpage_id"].map(
        lambda pid: image_map.get(pid, {}).get("status", "missing")
    )

    total = len(df)
    with_image = (df["Status"] == "success").sum()
    blocked = (df["Status"] == "blocked").sum()
    blank = (df["Status"] == "blank").sum()
    error = (df["Status"] == "error").sum()
    missing = (df["Status"] == "missing").sum()

    print(f"Total: {total}")
    print(f"  Dengan screenshot : {with_image}")
    print(f"  Diblokir           : {blocked}")
    print(f"  Kosong             : {blank}")
    print(f"  Error              : {error}")
    print(f"  Belum diproses     : {missing}")

    df.to_csv(OUTPUT_PATH, index=False)
    print(f"\nDisimpan: {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
