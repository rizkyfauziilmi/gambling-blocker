import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse

from bs4 import BeautifulSoup

from custom_types.models import ScrapedRecord
from lib.config import ScraperConfig
from lib.fetcher import fetch_with_retry
from lib.html_extractor import (
    extract_body_text,
    extract_domain_ext,
    extract_meta,
    is_html_response,
    is_valid_url,
    normalize_url,
)

from .storage import load_existing_urls, save_to_csv, save_to_jsonl


def scrape_url(
    url: str, label: int, config: ScraperConfig, logger: logging.Logger
) -> Optional[ScrapedRecord]:
    """
    Scrape satu URL dan kembalikan ScrapedRecord.

    Args:
        url: URL yang akan di-scrape
        label: 1 = Gambling, 0 = Non-Gambling
        config: ScraperConfig instance
        logger: Logger instance

    Returns:
        ScrapedRecord jika berhasil, None jika gagal
    """
    if not is_valid_url(url):
        logger.error(f"URL tidak valid, dilewati: {url}")
        return None

    logger.info(f"Scraping [label={label}] → {url}")

    response = fetch_with_retry(
        url,
        timeout=config.timeout,
        max_retries=config.max_retries,
        retry_backoff=config.retry_backoff,
        headers=config.headers,
        logger=logger,
    )

    if response is None:
        logger.error(f"Gagal fetch setelah {config.max_retries} percobaan: {url}")
        return None

    if response.status_code != 200:
        logger.warning(f"HTTP {response.status_code}: {url}")
        return None

    # Pastikan response HTML
    if not is_html_response(response):
        logger.warning(
            f"Bukan HTML (Content-Type: {response.headers.get('Content-Type', '?')}): {url}"
        )
        return None

    html = response.text
    soup = BeautifulSoup(html, "html.parser")

    # Metadata URL (normalize untuk konsistensi)
    normalized_url = normalize_url(response.url)
    parsed = urlparse(normalized_url)
    domain = parsed.netloc
    domain_ext = extract_domain_ext(parsed.netloc)

    # Ekstraksi konten
    raw_title = soup.title.string.strip() if soup.title and soup.title.string else ""
    raw_meta_desc = extract_meta(soup)
    raw_body_text = extract_body_text(html, config.removed_tags)

    record = ScrapedRecord(
        timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        url=normalized_url,
        domain=domain,
        domain_ext=domain_ext,
        http_status=response.status_code,
        raw_title=raw_title,
        raw_meta_desc=raw_meta_desc,
        label=label,
    )

    logger.info(f"✓ OK | title='{raw_title[:50]}' | body={len(raw_body_text)}c | ")
    return record


def run_scraper(
    target_list: list[dict],
    config: ScraperConfig,
    logger: logging.Logger,
    resume: bool = True,
) -> list[ScrapedRecord]:
    """
    Jalankan scraping secara concurrent dengan progress bar dan resume capability.

    Args:
        target_list : list of dict {"url": str, "label": int}
        config      : ScraperConfig instance
        logger      : Logger instance
        resume      : jika True, skip URL yang sudah di-scrape dari dataset existing

    Returns:
        list of ScrapedRecord
    """

    # Load existing URLs jika resume mode aktif
    original_count = len(target_list)
    if resume:
        existing_urls = load_existing_urls(config)
        target_list = [
            item
            for item in target_list
            if normalize_url(item["url"]) not in existing_urls
        ]
        skipped = original_count - len(target_list)
        if skipped > 0:
            logger.info(
                f"Resume mode: {skipped} URL sudah di-scrape, {len(target_list)} tersisa"
            )

    if not target_list:
        logger.warning("Semua URL sudah di-scrape atau target list kosong.")
        return []

    logger.info(
        f"Memulai scraping {len(target_list)} URL dengan {config.max_workers} worker..."
    )

    results: list[ScrapedRecord] = []
    failed: list[str] = []

    def task(item: dict) -> Optional[ScrapedRecord]:
        record = scrape_url(item["url"], item["label"], config, logger)
        time.sleep(config.delay_between_requests)
        return record

    with ThreadPoolExecutor(max_workers=config.max_workers) as executor:
        futures = {executor.submit(task, item): item for item in target_list}
        completed = 0

        for future in as_completed(futures):
            completed += 1
            item = futures[future]
            try:
                record = future.result()
                if record:
                    results.append(record)
                else:
                    failed.append(item["url"])
            except Exception as e:
                logger.error(f"Unexpected error untuk {item['url']}: {e}")
                failed.append(item["url"])

            # Log progress setiap 10 item atau di akhir
            if completed % 10 == 0 or completed == len(futures):
                logger.info(
                    f"Progress: {completed}/{len(futures)} URL berhasil diproses"
                )

    if failed:
        logger.warning(f"{len(failed)} URL gagal / dilewati:")
        for u in failed:
            logger.warning(f"  ✗ {u}")

    if results:
        # Append mode jika resume dan file sudah ada
        should_append = resume and (
            (config.dataset_dir / config.csv_filename).exists()
            or (config.dataset_dir / config.jsonl_filename).exists()
        )
        save_to_csv(results, config, logger, append=should_append)
        save_to_jsonl(results, config, logger, append=should_append)

        # Print summary untuk batch saat ini
        logger.info(f"Batch sekarang: {len(results)} record berhasil")

        # Jika resume mode, tampilkan total dataset
        if resume:
            total_in_files = len(load_existing_urls(config))
            logger.info(f"Total dalam dataset sekarang: {total_in_files} record")
    else:
        logger.error("Tidak ada data yang berhasil diambil di batch ini.")

    return results
