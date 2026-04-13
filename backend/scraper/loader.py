import csv
import json
import logging
from pathlib import Path
from typing import Optional

from lib.config import SCRAPER_CONFIG, ScraperConfig
from lib.html_extractor import normalize_url

from .storage import load_existing_urls


def load_urls(
    filepath: str,
    logger: Optional[logging.Logger] = None,
    skip_existing: bool = False,
    config: Optional[ScraperConfig] = None,
) -> list[dict]:
    """
    Load target URLs dari file eksternal (CSV atau JSONL).

    Format CSV (dengan header):
        url,label
        https://site1.com,1
        https://site2.com,0

    Format JSONL:
        {"url": "https://site1.com", "label": 1}
        {"url": "https://site2.com", "label": 0}

    Args:
        filepath : path ke file CSV atau JSONL
        logger   : optional logger untuk warning/info
        skip_existing : jika True, filter out URLs yang sudah di-scrape di dataset
        config   : ScraperConfig instance (diperlukan jika skip_existing=True)

    Returns:
        list of dict {"url": str, "label": int}
    """
    if logger is None:
        logger = logging.getLogger("scraper_judi")

    target_list = []
    path = Path(filepath)

    if not path.exists():
        logger.error(f"File tidak ditemukan: {filepath}")
        return target_list

    try:
        if path.suffix.lower() == ".csv":
            with open(path, mode="r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row and row.get("url"):
                        try:
                            target_list.append(
                                {
                                    "url": normalize_url(row["url"].strip()),
                                    "label": int(row.get("label", 1)),
                                }
                            )
                        except (ValueError, KeyError) as e:
                            logger.warning(f"Gagal parse row: {row} → {e}")
            logger.info(f"CSV loaded: {len(target_list)} URLs dari {filepath}")

        elif path.suffix.lower() == ".jsonl":
            with open(path, mode="r", encoding="utf-8") as f:
                for idx, line in enumerate(f, 1):
                    try:
                        record = json.loads(line.strip())
                        if record and record.get("url"):
                            target_list.append(
                                {
                                    "url": normalize_url(record["url"].strip()),
                                    "label": int(record.get("label", 1)),
                                }
                            )
                    except (json.JSONDecodeError, ValueError) as e:
                        logger.warning(f"JSONL line {idx} invalid: {e}")
            logger.info(f"JSONL loaded: {len(target_list)} URLs dari {filepath}")

        else:
            logger.error(
                f"Format file tidak didukung: {path.suffix} (gunakan .csv atau .jsonl)"
            )
            return target_list

    except Exception as e:
        logger.error(f"Error membaca file {filepath}: {e}")
        return target_list

    # Filter out already-scraped URLs if skip_existing is True
    if skip_existing:
        if config is None:
            config = SCRAPER_CONFIG
        existing_urls = load_existing_urls(config)
        original_count = len(target_list)
        target_list = [item for item in target_list if item["url"] not in existing_urls]
        filtered_count = original_count - len(target_list)
        if filtered_count > 0:
            logger.info(
                f"Filtered {filtered_count} already-scraped URLs, {len(target_list)} new URLs to scrape"
            )

    return target_list
