import csv
import json
import logging
from dataclasses import asdict

from custom_types.models import CSV_FIELDS, ScrapedRecord
from lib.config import ScraperConfig
from lib.html_extractor import normalize_url


def load_existing_urls(config: ScraperConfig) -> set[str]:
    """Load URLs yang sudah di-scrape dari CSV atau JSONL untuk menghindari duplikasi."""
    existing_urls = set()

    csv_path = config.dataset_dir / config.csv_filename
    if csv_path.exists():
        try:
            with open(csv_path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row and row.get("url"):
                        existing_urls.add(normalize_url(row["url"]))
        except Exception as e:
            logging.getLogger("scraper_judi").warning(f"Error reading CSV: {e}")

    jsonl_path = config.dataset_dir / config.jsonl_filename
    if jsonl_path.exists():
        try:
            with open(jsonl_path, mode="r", encoding="utf-8") as f:
                for line in f:
                    try:
                        record = json.loads(line.strip())
                        if record and record.get("url"):
                            existing_urls.add(normalize_url(record["url"]))
                    except json.JSONDecodeError:
                        continue
        except Exception as e:
            logging.getLogger("scraper_judi").warning(f"Error reading JSONL: {e}")

    return existing_urls


def save_to_csv(
    records: list[ScrapedRecord],
    config: ScraperConfig,
    logger: logging.Logger,
    append: bool = False,
) -> None:
    """Simpan records ke CSV file."""
    config.dataset_dir.mkdir(parents=True, exist_ok=True)

    path = config.dataset_dir / config.csv_filename
    mode = "a" if (append and path.exists()) else "w"

    with open(path, mode=mode, newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        # Write header only if creating new file or not appending
        if mode == "w":
            writer.writeheader()
        for record in records:
            row = asdict(record)
            writer.writerow(row)  # type: ignore
    logger.info(f"CSV disimpan → {path} ({len(records)} baris ditambahkan)")


def save_to_jsonl(
    records: list[ScrapedRecord],
    config: ScraperConfig,
    logger: logging.Logger,
    append: bool = False,
) -> None:
    """Simpan records ke JSONL file."""
    config.dataset_dir.mkdir(parents=True, exist_ok=True)

    path = config.dataset_dir / config.jsonl_filename
    mode = "a" if (append and path.exists()) else "w"

    with open(path, mode=mode, encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(asdict(record), ensure_ascii=False) + "\n")
    logger.info(f"JSONL disimpan → {path} ({len(records)} baris ditambahkan)")
