import requests

from crawler.logger import crawler_log
from settings.crawler import CRAWLER_HEADERS, CRAWLER_SETTINGS

_XML_CONTENT_TYPES = {
    "application/xml",
    "text/xml",
    "application/rss+xml",
    "application/atom+xml",
}


def _is_xml_content_type(content_type: str) -> bool:
    """Kembalikan ``True`` jika header Content-Type menunjukkan respons XML."""
    return content_type.lower().split(";")[0].strip() in _XML_CONTENT_TYPES


def fetch(url: str) -> str | None:
    """Ambil URL dan kembalikan isi respons sebagai teks.

    Respons dengan content type XML dilewati karena tidak mengandung
    link HTML yang bisa di-crawl.

    Args:
        url: URL yang akan diambil.

    Returns:
        Isi respons sebagai string, atau ``None`` jika request gagal,
        timeout, atau mengembalikan status code bukan 200.
    """
    try:
        resp = requests.get(
            url,
            headers=CRAWLER_HEADERS,
            timeout=CRAWLER_SETTINGS["request_timeout"],
            allow_redirects=True,
        )
        if resp.status_code != 200:
            crawler_log(f"[HTTP {resp.status_code}] {url}")
            return None

        if _is_xml_content_type(resp.headers.get("Content-Type", "")):
            crawler_log(f"[SKIP XML] {url}")
            return None

        return resp.text

    except requests.Timeout:
        crawler_log(f"[TIMEOUT] {url}")
    except requests.ConnectionError:
        crawler_log(f"[CONN ERR] {url}")
    except requests.RequestException as e:
        crawler_log(f"[REQ ERR] {url}: {e}")

    return None
