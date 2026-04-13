from urllib.parse import urlparse

import requests
from bs4 import BeautifulSoup


def normalize_url(url: str) -> str:
    """
    Normalize URL untuk konsistensi dalam filtering duplikat.
    - Hapus www. prefix
    - Hapus trailing slashes
    - Normalisasi ke lowercase

    Contoh:
        https://www.kitabisa.com → https://kitabisa.com
        https://kitabisa.com/ → https://kitabisa.com
    """
    try:
        parsed = urlparse(url)
        # Hapus www. prefix untuk normalisasi
        netloc = parsed.netloc.lower()
        if netloc.startswith("www."):
            netloc = netloc[4:]

        # Reconstruct dengan format konsisten (tanpa trailing slash)
        normalized = f"{parsed.scheme}://{netloc}{parsed.path.rstrip('/')}"
        return normalized if normalized else url
    except Exception:
        return url


def is_valid_url(url: str) -> bool:
    """Validasi format URL."""
    try:
        parsed = urlparse(url)
        return parsed.scheme in ("http", "https") and bool(parsed.netloc)
    except Exception:
        return False


def is_html_response(response: requests.Response) -> bool:
    """Pastikan response adalah HTML, bukan PDF / JSON / binary."""
    content_type = response.headers.get("Content-Type", "")
    return "text/html" in content_type or "application/xhtml" in content_type


def extract_domain_ext(netloc: str) -> str:
    """
    Extract domain extension dari netloc (e.g., "example.co.uk" → "co.uk").

    Aturan:
    - Jika domain berakhir dengan suffix terkenal (co.uk, com.au, etc) → gunakan itu
    - Else → gunakan bagian terakhir (TLD biasa)
    """
    # Known multi-part TLDs
    known_suffixes = {
        "co.uk",
        "co.id",
        "co.kr",
        "co.jp",
        "co.th",
        "com.au",
        "com.br",
        "com.mx",
        "gov.uk",
        "ac.uk",
    }

    domain_lower = netloc.lower()
    # Hapus www. jika ada
    if domain_lower.startswith("www."):
        domain_lower = domain_lower[4:]

    # Cek apakah domain itu adalah IP address
    if domain_lower.replace(".", "").isdigit():
        return "ip"

    parts = domain_lower.split(".")
    if len(parts) < 2:
        return parts[-1] if parts else ""

    # Cek 2-part suffix terkenal
    if len(parts) >= 2:
        two_part = f"{parts[-2]}.{parts[-1]}"
        if two_part in known_suffixes:
            return two_part

    # Fallback: return terakhir saja
    candidate = parts[-1]
    if candidate and not candidate.isdigit():
        return candidate
    return parts[-1] if parts else ""


def extract_meta(soup: BeautifulSoup) -> str:
    """Extract meta description dari HTML."""
    meta_desc = soup.find("meta", attrs={"name": "description"})
    if meta_desc and meta_desc.get("content"):
        return str(meta_desc.get("content", "")).strip()

    meta_og = soup.find("meta", attrs={"property": "og:description"})
    if meta_og and meta_og.get("content"):
        return str(meta_og.get("content", "")).strip()

    return ""


def extract_body_text(html: str, removed_tags: tuple) -> str:
    """
    Extract plain text dari HTML body, filter tag yang tidak penting.

    removed_tags: tuple of tag names untuk dibuang sebelum extract
    """
    soup = BeautifulSoup(html, "html.parser")

    # Buang tag yang tidak perlu
    for tag_name in removed_tags:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    # Ambil text dari body atau keseluruhan soup
    body = soup.find("body")
    text_content = (body.get_text() if body else soup.get_text()).strip()

    # Clean whitespace
    lines = [line.strip() for line in text_content.split("\n") if line.strip()]
    return " ".join(lines)
