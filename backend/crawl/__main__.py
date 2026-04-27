import logging
import queue as _queue
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from colorama import init
from tqdm import tqdm

from .links import DOMAINS_TO_CRAWL

init(autoreset=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SETTINGS = {
    "max_pages_per_domain": 200,
    "delay_seconds": 1.0,
    "request_timeout": 15,
    "respect_robots_txt": False,
    "max_workers": 5,
    "output_csv": "dataset_crawl.csv",
    "output_json": "dataset_crawl.json",
    "log_file": "crawler.log",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

# ══════════════════════════════════════════════════════════════════════════════
#  FILE LOGGER
#  Semua log ditulis ke file, bukan ke stdout, agar tqdm tidak rusak.
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    filename=SETTINGS["log_file"],
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("crawler")


def log(msg: str = "") -> None:
    """Tulis satu baris ke log file (bukan stdout)."""
    # Strip colorama escape codes sebelum masuk file
    clean = re.sub(r"\x1b\[[0-9;]*m", "", msg)
    _logger.info(clean)


def log_block(*lines: str) -> None:
    """Tulis beberapa baris sekaligus ke log file secara atomik."""
    clean_lines = [re.sub(r"\x1b\[[0-9;]*m", "", ln) for ln in lines]
    _logger.info("\n".join(clean_lines))


# ══════════════════════════════════════════════════════════════════════════════
#  TQDM THREAD SAFETY
#  Harus dipanggil sekali sebelum ThreadPoolExecutor dibuat.
# ══════════════════════════════════════════════════════════════════════════════

tqdm.set_lock(threading.RLock())

# ══════════════════════════════════════════════════════════════════════════════
#  THREAD-SAFE PRIMITIVES
# ══════════════════════════════════════════════════════════════════════════════


class AtomicCounter:
    """Counter integer yang thread-safe menggunakan threading.Lock()."""

    def __init__(self, start: int = 1) -> None:
        self._value = start
        self._lock = threading.Lock()

    def next(self) -> int:
        with self._lock:
            val = self._value
            self._value += 1
            return val

    @property
    def value(self) -> int:
        with self._lock:
            return self._value


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION
# ══════════════════════════════════════════════════════════════════════════════


def validate_domains(domains: list[tuple[str, str]]) -> None:
    seen: dict[str, str] = {}
    duplicates: list[str] = []

    for url, _ in domains:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        if netloc in seen:
            duplicates.append(f"  • '{url}'  ↔  '{seen[netloc]}'")
        else:
            seen[netloc] = url

    if duplicates:
        raise ValueError(
            "\n[DUPLICATE DOMAIN ERROR] DOMAINS_TO_CRAWL mengandung domain duplikat:\n"
            + "\n".join(duplicates)
            + "\nHapus salah satu sebelum menjalankan crawler."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  DATASET  (baca / resume)
# ══════════════════════════════════════════════════════════════════════════════


def load_existing_dataset(csv_path: str) -> pd.DataFrame:
    path = Path(csv_path)
    if not path.exists():
        log("[~] Tidak ada dataset lama ditemukan. Mulai dari awal.")
        return pd.DataFrame(
            columns=["Webpage_id", "Domain", "Url", "Tag", "Crawled_at"]
        )

    df = pd.read_csv(path)

    before = len(df)
    df = df.drop_duplicates(subset=["Url"], keep="first")
    dupes = before - len(df)
    if dupes:
        log(f"[!] {dupes} baris duplikat dihapus dari dataset lama.")

    return df


def get_visited_urls_per_domain(df: pd.DataFrame) -> dict[str, set[str]]:
    if df.empty:
        return {}
    mapping: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        domain = str(row["Domain"])
        url = str(row["Url"])
        mapping.setdefault(domain, set()).add(url)
    return mapping


# ══════════════════════════════════════════════════════════════════════════════
#  URL UTILITIES
# ══════════════════════════════════════════════════════════════════════════════

SKIP_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",
    ".pdf",
    ".zip",
    ".rar",
    ".exe",
    ".mp4",
    ".mp3",
    ".css",
    ".js",
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",
    ".xml",
    ".json",
}

SKIP_PATTERNS = re.compile(
    r"(mailto:|javascript:|tel:|#|/logout|/cdn-cgi/|/wp-json/|/feed/|\.rss$|/sitemap|\.xml$)"
)


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    clean_url = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
    clean_url = clean_url.rstrip("/")
    return clean_url.lower()


def is_root_domain(url: str) -> bool:
    path = urlparse(url).path
    return path in ("", "/")


def is_valid_url(url: str, base_domain: str) -> bool:
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
    return True


def extract_links(html: str, base_url: str, base_domain: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = []
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        full_url = urljoin(base_url, href)
        full_url = normalize_url(full_url)
        if is_valid_url(full_url, base_domain):
            links.append(full_url)
    return links


# ══════════════════════════════════════════════════════════════════════════════
#  HTTP FETCHER
# ══════════════════════════════════════════════════════════════════════════════

XML_CONTENT_TYPES = {
    "application/xml",
    "text/xml",
    "application/rss+xml",
    "application/atom+xml",
}


def is_xml_content_type(content_type: str) -> bool:
    ct = content_type.lower().split(";")[0].strip()
    return ct in XML_CONTENT_TYPES


def fetch(url: str) -> str | None:
    try:
        resp = requests.get(
            url,
            headers=HEADERS,
            timeout=SETTINGS["request_timeout"],
            allow_redirects=True,
        )

        if resp.status_code == 200:
            content_type = resp.headers.get("Content-Type", "")
            if is_xml_content_type(content_type):
                log(f"[SKIP XML] {url}  ({content_type.split(';')[0].strip()})")
                return None
            return resp.text
        else:
            log(f"[{resp.status_code}] Tidak ditangani: {url}")
            return None

    except requests.Timeout:
        log(f"[TIMEOUT]: {url}")
        return None
    except requests.ConnectionError:
        log(f"[CONN ERR] {url}")
        return None
    except requests.RequestException as e:
        log(f"[ERR] {url}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  BFS DOMAIN CRAWLER
# ══════════════════════════════════════════════════════════════════════════════


def crawl_domain(
    start_url: str,
    category: str,
    already_visited: set[str],
    counter: AtomicCounter,
    worker_index: int,
    pos_queue: _queue.Queue,  # ← pool posisi tqdm yang di-recycle
) -> list[dict]:
    """
    BFS crawler untuk satu domain.

    Mengambil satu slot posisi dari pos_queue sebelum membuka tqdm bar,
    dan mengembalikannya setelah bar ditutup — sehingga posisi selalu
    dalam rentang [0, max_workers) dan tidak pernah sparse.
    """
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc
    max_pages = SETTINGS["max_pages_per_domain"]

    already_count = len(already_visited)
    remaining_quota = (max_pages - already_count) if max_pages else None

    log_block(
        "",
        f"{'═' * 60}",
        f"  [Worker {worker_index}] Crawling : {start_url}",
        f"  Domain   : {base_domain}",
        f"  Sudah di-crawl  : {already_count} halaman (dari dataset lama)",
        f"  Sisa kuota      : {remaining_quota if remaining_quota is not None else 'unlimited'}",
        f"{'═' * 60}",
    )

    if remaining_quota is not None and remaining_quota <= 0:
        log(f"[!] Kuota {max_pages} halaman sudah habis untuk {base_domain}. Lewati.")
        return []

    visited: set[str] = set(already_visited)
    start = normalize_url(start_url)
    queue: deque[str] = deque([start])
    queued: set[str] = {start}
    records: list[dict] = []

    # ── Ambil posisi dari pool sebelum membuka tqdm ───────────────────────────
    # Blok di sini sampai ada slot kosong (dijamin oleh ukuran pool = max_workers)
    bar_pos = pos_queue.get()

    try:
        with tqdm(
            desc=f"  [W{worker_index:>2}] {base_domain:<30}",
            unit=" pg",
            colour="cyan",
            position=bar_pos,  # posisi selalu 0..max_workers-1
            leave=False,  # hapus bar saat selesai → slot bebas dipakai ulang
            dynamic_ncols=True,
            miniters=0,
            mininterval=0.1,
        ) as pbar:
            while queue:
                if remaining_quota is not None and len(records) >= remaining_quota:
                    log(
                        f"[W{worker_index}] Kuota {remaining_quota} halaman tercapai untuk {base_domain}."
                    )
                    break

                current_url = queue.popleft()

                if current_url in visited:
                    continue
                visited.add(current_url)

                is_root = is_root_domain(current_url)

                urls_in_pipeline = len(records) + len(queue)
                needs_fetch = is_root or (
                    remaining_quota is None or urls_in_pipeline < remaining_quota
                )

                if needs_fetch:
                    html = fetch(current_url)
                    time.sleep(SETTINGS["delay_seconds"])

                    if html is None:
                        continue

                    for link in extract_links(html, current_url, base_domain):
                        if link not in queued:
                            queue.append(link)
                            queued.add(link)

                if is_root:
                    continue

                webpage_id = counter.next()
                records.append(
                    {
                        "Webpage_id": webpage_id,
                        "Domain": base_domain,
                        "Url": current_url,
                        "Tag": category,
                        "Crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    }
                )

                mode = "fetch" if needs_fetch else "drain"
                pbar.update(1)
                pbar.set_postfix(
                    queue=len(queue),
                    new=len(records),
                    total=already_count + len(records),
                    mode=mode,
                )

    finally:
        # ── Kembalikan posisi ke pool setelah bar ditutup ─────────────────────
        pos_queue.put(bar_pos)

    min_pages_required = 1
    if len(records) < min_pages_required:
        log(
            f"[SKIP] {base_domain} diabaikan karena hanya ditemukan "
            f"{len(records)} halaman internal (kurang dari {min_pages_required})."
        )
        return []

    log(f"[W{worker_index}] Selesai: {len(records)} halaman baru dari {base_domain}")
    return records


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-DOMAIN RUNNER
# ══════════════════════════════════════════════════════════════════════════════


def run(domains: list[tuple[str, str]], existing_df: pd.DataFrame) -> pd.DataFrame:
    visited_map = get_visited_urls_per_domain(existing_df)

    id_start = int(existing_df["Webpage_id"].max()) + 1 if not existing_df.empty else 1
    counter = AtomicCounter(start=id_start)

    all_new_records: list[dict] = []

    configured = SETTINGS.get("max_workers")
    max_workers = min(configured, len(domains)) if configured else len(domains)

    log(f"Meluncurkan {max_workers} worker thread(s) (dari {len(domains)} domain)…")

    # ── Pool posisi tqdm: hanya max_workers slot (0..max_workers-1) ───────────
    # Setiap worker mengambil satu slot sebelum membuka bar-nya,
    # dan mengembalikannya setelah bar ditutup.
    # Ini menjamin posisi selalu padat dan tidak pernah sparse.
    pos_queue: _queue.Queue = _queue.Queue()
    for i in range(max_workers):
        pos_queue.put(i)

    future_to_meta: dict = {}

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for idx, (url, category) in enumerate(domains):
            netloc = urlparse(url).netloc
            already_visited = visited_map.get(netloc, set())

            future = executor.submit(
                crawl_domain,
                start_url=url,
                category=category,
                already_visited=already_visited,
                counter=counter,
                worker_index=idx,
                pos_queue=pos_queue,  # ← diteruskan ke setiap worker
            )
            future_to_meta[future] = (url, idx)

        for future in as_completed(future_to_meta):
            url, idx = future_to_meta[future]
            try:
                new_records = future.result()
                all_new_records.extend(new_records)
            except Exception as exc:
                log(f"[Worker {idx}] Exception untuk {url}: {exc}")

    if all_new_records:
        new_df = pd.DataFrame(all_new_records)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = existing_df.copy()
        log("[~] Tidak ada halaman baru yang di-crawl.")

    return combined_df


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════


def export(df: pd.DataFrame) -> None:
    if df.empty:
        log("[!] Dataset kosong, tidak ada yang diekspor.")
        return

    df = df.sort_values("Webpage_id").reset_index(drop=True)

    df.to_csv(SETTINGS["output_csv"], index=False)
    log(f"✓ CSV  → {SETTINGS['output_csv']}  ({len(df)} baris)")

    df.to_json(SETTINGS["output_json"], orient="records", indent=2)
    log(f"✓ JSON → {SETTINGS['output_json']}")

    sep = "─" * 60
    log_block(
        "",
        sep,
        "  Preview dataset (5 baris terakhir):",
        sep,
        df[["Webpage_id", "Domain", "Url", "Tag"]].tail(5).to_string(index=True),
        "",
        sep,
        "  Statistik:",
        f"  Total halaman crawled : {len(df)}",
        f"  Jumlah domain         : {df['Domain'].nunique()}",
        "  Distribusi tag:",
        *[
            f"    {tag:<15} {count:>4}  {'█' * (count * 20 // len(df))}"
            for tag, count in df["Tag"].value_counts().items()
        ],
        sep,
    )


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log_block(
        "",
        f"{'═' * 60}",
        "  Multi-Domain Web Crawler & Dataset Builder  [Multithreaded]",
        f"  Domains : {len(DOMAINS_TO_CRAWL)}",
        f"  Workers : {SETTINGS['max_workers']}  (max per batch)",
        f"{'═' * 60}",
    )

    validate_domains(DOMAINS_TO_CRAWL)

    existing_df = load_existing_dataset(SETTINGS["output_csv"])

    df = run(DOMAINS_TO_CRAWL, existing_df)

    export(df)
