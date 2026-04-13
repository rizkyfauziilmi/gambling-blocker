import logging
import time
from typing import Optional

import requests


def fetch_with_retry(
    url: str,
    timeout: int = 15,
    max_retries: int = 3,
    retry_backoff: float = 2.0,
    headers: Optional[dict] = None,
    logger: Optional[logging.Logger] = None,
) -> Optional[requests.Response]:
    """
    Fetch URL dengan retry logic dan exponential backoff.

    Args:
        url: URL yang akan di-fetch
        timeout: Request timeout dalam detik
        max_retries: Jumlah maksimal retry
        retry_backoff: Faktor pengali untuk backoff (exponential)
        headers: Custom HTTP headers
        logger: Logger instance

    Returns:
        requests.Response jika berhasil, None jika gagal
    """
    if logger is None:
        logger = logging.getLogger("scraper_judi")

    if headers is None:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        }

    attempt = 0
    while attempt < max_retries:
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers=headers,
                allow_redirects=True,
            )
            response.raise_for_status()
            return response

        except requests.exceptions.Timeout:
            attempt += 1
            if attempt < max_retries:
                wait_time = retry_backoff**attempt
                logger.warning(
                    f"Timeout untuk {url} (attempt {attempt}/{max_retries}), "
                    f"retry dalam {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Timeout setelah {max_retries} percobaan: {url}")

        except requests.exceptions.ConnectionError:
            attempt += 1
            if attempt < max_retries:
                wait_time = retry_backoff**attempt
                logger.warning(
                    f"Connection error untuk {url} (attempt {attempt}/{max_retries}), "
                    f"retry dalam {wait_time}s..."
                )
                time.sleep(wait_time)
            else:
                logger.error(f"Connection error setelah {max_retries} percobaan: {url}")

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP Error untuk {url}: {e}")
            return None

        except Exception as e:
            logger.error(f"Unexpected error saat fetch {url}: {e}")
            return None

    return None
