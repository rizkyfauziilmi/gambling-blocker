import threading
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from crawler.logger import crawler_log
from settings.crawler import MAX_URL_LENGTH, SKIP_EXTENSIONS, SKIP_PATTERNS


class AtomicCounter:
    """Counter integer yang naik secara monoton dan thread-safe.

    Args:
        start: Nilai awal counter (inklusif). Default ``1``.
    """

    def __init__(self, start: int = 1) -> None:
        self._value = start
        self._lock = threading.Lock()

    def next(self) -> int:
        """Kembalikan nilai saat ini secara atomik, lalu tambahkan satu."""
        with self._lock:
            val = self._value
            self._value += 1
            return val

    @property
    def value(self) -> int:
        """Snapshot nilai counter saat ini (hanya baca)."""
        with self._lock:
            return self._value


def validate_domains(domains: list[tuple[str, str]]) -> None:
    """Lempar ``ValueError`` jika ``DOMAINS_TO_CRAWL`` mengandung netloc duplikat.

    Args:
        domains: Daftar pasangan ``(url, category)`` yang akan divalidasi.

    Raises:
        ValueError: Jika dua atau lebih entri memiliki netloc yang sama.
    """
    seen: dict[str, str] = {}
    duplicates: list[str] = []

    for url, _ in domains:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        if netloc in seen:
            duplicates.append(f"  '{url}'  <->  '{seen[netloc]}'")
        else:
            seen[netloc] = url

    if duplicates:
        raise ValueError(
            "\n[DUPLICATE DOMAIN ERROR] DOMAINS_TO_CRAWL mengandung domain duplikat:\n"
            + "\n".join(duplicates)
            + "\nHapus salah satu entri sebelum menjalankan crawler."
        )


def has_undefined_segment(url: str) -> bool:
    """Kembalikan ``True`` jika ada segmen path yang secara harfiah berisi ``"undefined"``."""
    try:
        return any(s.lower() == "undefined" for s in urlparse(url).path.split("/") if s)
    except Exception:
        return False


def has_path_repetition(url: str) -> bool:
    """Deteksi pola crawler-trap yang disebabkan oleh pengulangan segmen path.

    Dua pola yang diperiksa:
        1. Segmen berurutan yang identik: ``/foo/foo/…``
        2. Bigram (pasangan segmen) yang muncul lagi: ``/a/b/…/a/b/…``

    Satu pengulangan pun sudah cukup untuk mengembalikan ``True``;
    tidak ada ambang batas.

    Args:
        url: URL yang akan diperiksa.

    Returns:
        ``True`` jika pola pengulangan terdeteksi, ``False`` jika tidak.
    """
    try:
        segs = [s.lower() for s in urlparse(url).path.split("/") if s]
        seen_bigrams: set[tuple] = set()

        for i in range(1, len(segs)):
            if segs[i] == segs[i - 1]:  # pola 1: segmen berurutan sama
                return True

            bigram = (segs[i - 1], segs[i])
            if bigram in seen_bigrams:  # pola 2: bigram berulang
                return True
            seen_bigrams.add(bigram)

        return False
    except Exception:
        return False


def normalize_url(url: str) -> str:
    """Kembalikan URL yang sudah dinormalisasi: huruf kecil, tanpa query string dan fragment."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/").lower()


def is_root_domain(url: str) -> bool:
    """Kembalikan ``True`` jika path URL kosong atau berupa ``"/"``."""
    return urlparse(url).path in ("", "/")


def is_valid_url(url: str, base_domain: str) -> bool:
    """Kembalikan ``True`` jika URL aman dan dalam cakupan untuk di-crawl.

    Sebuah URL ditolak apabila:
    - Menggunakan skema selain HTTP(S).
    - Mengarah ke luar domain target.
    - Memiliki ekstensi file yang diabaikan (gambar, font, arsip, dll.).
    - Cocok dengan pola skip yang diketahui (feed, rute CDN, endpoint JS/JSON).
    - Melebihi panjang URL maksimum yang diizinkan.
    - Mengandung segmen path ``"undefined"``.
    - Mengandung pola path berulang (crawler trap).

    Args:
        url: URL kandidat yang akan diperiksa.
        base_domain: Netloc dari domain yang sedang di-crawl.

    Returns:
        ``True`` jika URL harus ditambahkan ke antrian crawl.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        return False
    if base_domain not in parsed.netloc:
        return False
    ext = (
        "." + url.split(".")[-1].split("?")[0].lower()
        if "." in url.split("/")[-1]
        else ""
    )
    if ext in SKIP_EXTENSIONS:
        return False
    if SKIP_PATTERNS.search(url):
        return False
    if len(url) > MAX_URL_LENGTH:
        crawler_log(f"[SKIP terlalu-panjang] {url[:80]}...")
        return False
    if has_undefined_segment(url):
        crawler_log(f"[SKIP undefined] {url[:80]}...")
        return False
    if has_path_repetition(url):
        crawler_log(f"[SKIP repetisi] {url[:80]}...")
        return False
    return True


def extract_links(html: str, base_url: str, base_domain: str) -> list[str]:
    """Parse tag ``<a href>`` dan kembalikan URL yang valid, dalam cakupan, dan ternormalisasi.

    Args:
        html: Sumber HTML mentah dari halaman yang di-crawl.
        base_url: URL halaman yang sedang di-parse (untuk meresolusi href relatif).
        base_domain: Netloc dari domain yang sedang di-crawl.

    Returns:
        Daftar URL yang sudah dinormalisasi dan lolos :func:`is_valid_url`.
    """
    soup = BeautifulSoup(html, "html.parser")
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        full_url = normalize_url(urljoin(base_url, str(a["href"]).strip()))
        if is_valid_url(full_url, base_domain):
            links.append(full_url)
    return links
