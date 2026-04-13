#!/usr/bin/env python3
"""
Main entry point untuk scraper dengan modular architecture.

Usage:
    uv run scraper/main.py
    python scraper/main.py
    python -m scraper.main
"""

import logging
from pathlib import Path

from lib.config import SCRAPER_CONFIG
from lib.logger import setup_logging

from .loader import load_urls
from .scraper_engine import run_scraper


def main():
    """Main function untuk menjalankan scraper."""
    # Construct path relative to script location
    script_dir = Path(__file__).parent
    urls_file = script_dir / "constant" / "urls.csv"
    logger = setup_logging(name="scraper", log_level=logging.DEBUG)

    # Load target URLs dari input file
    target_list = load_urls(
        str(urls_file),
        skip_existing=True,
        config=SCRAPER_CONFIG,
    )

    # Jalankan scraping
    if target_list:
        run_scraper(target_list, config=SCRAPER_CONFIG, resume=True, logger=logger)
    else:
        logger.info("Tidak ada URL yang perlu di-scrape.")


if __name__ == "__main__":
    main()
