import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag
from collections import deque
import pandas as pd
import time
import re
import threading
from datetime import datetime
from tqdm import tqdm
from colorama import Fore, init
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from .links import DOMAINS_TO_CRAWL

init(autoreset=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SETTINGS = {
    "max_pages_per_domain": 200,
    "delay_seconds": 1.0,
    "request_timeout": 15,
    "max_retries": 2,
    "respect_robots_txt": False,
    "max_workers": 5,
    "output_csv": "dataset_crawl.csv",
    "output_json": "dataset_crawl.json",
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
#  THREAD-SAFE LOGGING
# ══════════════════════════════════════════════════════════════════════════════

# Dipakai untuk mengelompokkan blok multi-baris agar tidak tersisip thread lain.
# Untuk single-line log, tqdm.write() sudah thread-safe sendiri tanpa lock ini.
_print_lock = threading.Lock()


def log(msg: str = "") -> None:
    """
    Single-line log yang aman di tengah tqdm progress bars.

    tqdm.write() membekukan semua progress bar sementara, menulis satu baris,
    lalu mengembalikan cursor — sehingga bar tidak rusak/terpotong.
    Sudah thread-safe secara internal; tidak perlu lock untuk satu baris.
    """
    tqdm.write(msg)


def log_block(*lines: str) -> None:
    """
    Multi-line log yang ditulis secara atomik.

    Semua baris dikumpulkan terlebih dahulu menjadi satu string,
    lalu dikirim ke tqdm.write() dalam satu panggilan dengan _print_lock.
    Ini mencegah thread lain menyisipkan output di antara baris-baris header domain.

    Contoh penggunaan:
        log_block(
            f"{'═' * 60}",
            f"  [Worker {idx}] Crawling : {url}",
            f"  Domain   : {domain}",
            f"{'═' * 60}",
        )
    """
    combined = "\n".join(lines)
    with _print_lock:
        tqdm.write(combined)


# ══════════════════════════════════════════════════════════════════════════════
#  THREAD-SAFE PRIMITIVES
# ══════════════════════════════════════════════════════════════════════════════


class AtomicCounter:
    """
    Counter integer yang thread-safe menggunakan threading.Lock().

    Setiap panggilan ke `next()` menjamin nilai yang unik dan berurutan
    meskipun dipanggil secara bersamaan oleh banyak thread.
    """

    def __init__(self, start: int = 1) -> None:
        self._value = start
        self._lock = threading.Lock()

    def next(self) -> int:
        """Kembalikan nilai saat ini lalu increment secara atomik."""
        with self._lock:
            val = self._value
            self._value += 1
            return val

    @property
    def value(self) -> int:
        """Baca nilai saat ini tanpa increment (thread-safe)."""
        with self._lock:
            return self._value


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION
# ══════════════════════════════════════════════════════════════════════════════


def validate_domains(domains: list[tuple[str, str]]) -> None:
    """
    Lempar ValueError jika ada domain duplikat di DOMAINS_TO_CRAWL.
    Normalisasi dengan netloc agar http vs https atau trailing slash tidak lolos.
    """
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
    """
    Baca dataset CSV yang sudah ada.
    Kembalikan DataFrame kosong jika file belum ada.
    """
    path = Path(csv_path)
    if not path.exists():
        log(f"\n{Fore.YELLOW}[~] Tidak ada dataset lama ditemukan. Mulai dari awal.")
        return pd.DataFrame(
            columns=["Webpage_id", "Domain", "Url", "Tag", "Crawled_at"]
        )

    df = pd.read_csv(path)

    before = len(df)
    df = df.drop_duplicates(subset=["Url"], keep="first")
    dupes = before - len(df)
    if dupes:
        log(f"  {Fore.YELLOW}[!] {dupes} baris duplikat dihapus dari dataset lama.")

    return df


def get_visited_urls_per_domain(df: pd.DataFrame) -> dict[str, set[str]]:
    """
    Bangun mapping netloc → set(url) dari dataset yang sudah ada.
    """
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
    r"(mailto:|javascript:|tel:|#|/login|/logout|/signup|/register|"
    r"/cdn-cgi/|/wp-json/|/feed/|\.rss$|/sitemap)"
)


def normalize_url(url: str) -> str:
    url, _ = urldefrag(url)
    url = url.rstrip("/")
    return url.lower()


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
#  HTTP FETCHER (with retry)
# ══════════════════════════════════════════════════════════════════════════════

RETRYABLE_STATUS = {429, 500, 502, 503, 504}

XML_CONTENT_TYPES = {
    "application/xml",
    "text/xml",
    "application/rss+xml",
    "application/atom+xml",
}


def is_xml_content_type(content_type: str) -> bool:
    ct = content_type.lower().split(";")[0].strip()
    return ct in XML_CONTENT_TYPES


def fetch(url: str, retries: int = SETTINGS["max_retries"]) -> str | None:
    """
    Fetch satu URL dengan retry logic.
    Semua log dialihkan ke log() agar tidak merusak tqdm bars.
    """
    for attempt in range(retries + 1):
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
                    log(
                        f"  {Fore.YELLOW}[SKIP XML] {url}  "
                        f"({content_type.split(';')[0].strip()})"
                    )
                    return None
                return resp.text

            elif resp.status_code in RETRYABLE_STATUS:
                wait = int(resp.headers.get("Retry-After", 2**attempt))
                log(
                    f"  {Fore.YELLOW}[{resp.status_code}] Retry {attempt + 1}/{retries} "
                    f"dalam {wait}s: {url}"
                )
                if attempt < retries:
                    time.sleep(wait)
                else:
                    log(f"  {Fore.RED}[SKIP] Menyerah setelah {retries} retry: {url}")
                    return None

            elif resp.status_code == 403:
                log(f"  {Fore.YELLOW}[403] Forbidden (tidak di-retry): {url}")
                return None

            elif resp.status_code == 404:
                return None

            else:
                log(f"  {Fore.YELLOW}[{resp.status_code}] Tidak ditangani: {url}")
                return None

        except requests.Timeout:
            log(f"  {Fore.YELLOW}[TIMEOUT] attempt {attempt + 1}/{retries + 1}: {url}")
            if attempt < retries:
                time.sleep(2**attempt)
            else:
                log(f"  {Fore.RED}[SKIP] Timeout habis: {url}")

        except requests.ConnectionError:
            log(f"  {Fore.RED}[CONN ERR] {url}")
            if attempt < retries:
                time.sleep(2**attempt)

        except requests.RequestException as e:
            log(f"  {Fore.RED}[ERR] {url}: {e}")
            if attempt < retries:
                time.sleep(2**attempt)

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
) -> list[dict]:
    """
    BFS crawler untuk satu domain. Dijalankan sebagai satu thread.

    Semua output log dialihkan ke log() / log_block() agar tidak
    menabrak progress bar tqdm dari thread lain.
    """
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc
    max_pages = SETTINGS["max_pages_per_domain"]

    already_count = len(already_visited)
    remaining_quota = (max_pages - already_count) if max_pages else None

    # ── Header domain: ditulis atomik agar tidak tersisip thread lain ────────
    quota_line = (
        f"  Sisa kuota      : {remaining_quota} halaman"
        if remaining_quota is not None
        else "  Sisa kuota      : unlimited"
    )
    log_block(
        "",
        f"{Fore.CYAN}{'═' * 60}",
        f"  [Worker {worker_index}] Crawling : {Fore.WHITE}{start_url}",
        f"{Fore.CYAN}  Domain   : {base_domain}",
        f"  Sudah di-crawl  : {already_count} halaman (dari dataset lama)",
        quota_line,
        f"{Fore.CYAN}{'═' * 60}",
    )

    if remaining_quota is not None and remaining_quota <= 0:
        log(
            f"  {Fore.YELLOW}[!] Kuota {max_pages} halaman sudah habis "
            f"untuk domain ini. Lewati."
        )
        return []

    visited: set[str] = set(already_visited)
    start = normalize_url(start_url)
    queue: deque[str] = deque([start])
    queued: set[str] = {start}  # URL yang sudah pernah masuk queue (visited ∪ queue)
    records: list[dict] = []

    # ── Satu progress bar per worker; position memastikan baris terpisah ──────
    # miniters=0 + mininterval=0 : update bar setiap kali pbar.update() dipanggil
    # tanpa menunggu interval, sehingga angka selalu akurat.
    # dynamic_ncols menyesuaikan lebar jika terminal di-resize.
    with tqdm(
        desc=f"  [W{worker_index}] {base_domain:<30}",
        unit=" hal",
        colour="cyan",
        position=worker_index,
        leave=True,
        dynamic_ncols=True,
        miniters=0,
        mininterval=0.1,
    ) as pbar:
        while queue:
            # ── Quota check ───────────────────────────────────────────────────
            if remaining_quota is not None and len(records) >= remaining_quota:
                log(
                    f"\n  {Fore.YELLOW}[W{worker_index}] "
                    f"Kuota {remaining_quota} halaman tercapai."
                )
                break

            current_url = queue.popleft()

            if current_url in visited:
                continue
            visited.add(current_url)

            is_root = is_root_domain(current_url)

            # ── Keputusan fetch atau skip ─────────────────────────────────────
            #
            # urls_in_pipeline = URL yang sudah tersimpan + yang masih antri.
            # Ini adalah estimasi berapa URL yang bisa kita rekam tanpa fetch lagi.
            #
            # FETCH jika:
            #   a) ini root domain → selalu fetch untuk seed link awal
            #   b) quota unlimited  → selalu fetch
            #   c) pipeline belum cukup → perlu lebih banyak kandidat URL
            #
            # SKIP FETCH jika:
            #   pipeline sudah >= quota → queue cukup, tidak perlu eksplorasi lagi.
            #   URL ini tetap direkam karena sudah ditemukan dari halaman valid.
            #
            urls_in_pipeline = len(records) + len(queue)
            needs_fetch = is_root or (
                remaining_quota is None or urls_in_pipeline < remaining_quota
            )

            if needs_fetch:
                html = fetch(current_url)
                time.sleep(SETTINGS["delay_seconds"])

                if html is None:
                    # Fetch gagal (404, timeout, dll) → skip, jangan rekam URL ini
                    continue

                for link in extract_links(html, current_url, base_domain):
                    if link not in queued:
                        queue.append(link)
                        queued.add(link)
            # else: pipeline sudah cukup — percaya URL ini valid (ditemukan dari
            #       halaman yang berhasil di-fetch sebelumnya), langsung rekam.

            # Root domain: titik seed saja, tidak direkam
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

    status = f"{Fore.GREEN}✓" if records else f"{Fore.YELLOW}–"
    log(
        f"\n  {status} [Worker {worker_index}] Selesai: "
        f"{len(records)} halaman baru dari {base_domain}"
    )
    return records


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-DOMAIN RUNNER  (paralel via ThreadPoolExecutor)
# ══════════════════════════════════════════════════════════════════════════════


def run(domains: list[tuple[str, str]], existing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Submit setiap domain sebagai task ke ThreadPoolExecutor.
    Satu thread per domain berjalan paralel.
    """
    visited_map = get_visited_urls_per_domain(existing_df)

    id_start = int(existing_df["Webpage_id"].max()) + 1 if not existing_df.empty else 1
    counter = AtomicCounter(start=id_start)

    all_new_records: list[dict] = []

    configured = SETTINGS.get("max_workers")
    max_workers = min(configured, len(domains)) if configured else len(domains)

    log(
        f"\n{Fore.CYAN}  Meluncurkan {max_workers} worker thread(s) "
        f"(dari {len(domains)} domain)…{Fore.RESET}"
    )

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
            )
            future_to_meta[future] = (url, idx)

        for future in as_completed(future_to_meta):
            url, idx = future_to_meta[future]
            try:
                new_records = future.result()
                all_new_records.extend(new_records)
            except Exception as exc:
                log(f"\n{Fore.RED}  [Worker {idx}] Exception untuk {url}: {exc}")

    if all_new_records:
        new_df = pd.DataFrame(all_new_records)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = existing_df.copy()
        log(f"\n{Fore.YELLOW}[~] Tidak ada halaman baru yang di-crawl.")

    return combined_df


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════


def export(df: pd.DataFrame) -> None:
    if df.empty:
        log(f"\n{Fore.RED}[!] Dataset kosong, tidak ada yang diekspor.")
        return

    df = df.sort_values("Webpage_id").reset_index(drop=True)

    df.to_csv(SETTINGS["output_csv"], index=False)
    log(f"\n{Fore.GREEN}✓ CSV  → {SETTINGS['output_csv']}  ({len(df)} baris)")

    df.to_json(SETTINGS["output_json"], orient="records", indent=2)
    log(f"{Fore.GREEN}✓ JSON → {SETTINGS['output_json']}")

    preview_cols = ["Webpage_id", "Domain", "Url", "Tag"]
    sep = "─" * 60
    log_block(
        "",
        sep,
        "  Preview dataset (5 baris terakhir):",
        sep,
        df[preview_cols].tail(5).to_string(index=True),
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
        f"{Fore.CYAN}{'═' * 60}",
        "  Multi-Domain Web Crawler & Dataset Builder  [Multithreaded]",
        f"  Domains : {len(DOMAINS_TO_CRAWL)}",
        f"  Workers : {SETTINGS['max_workers']}  (max per batch)",
        f"{Fore.CYAN}{'═' * 60}",
    )

    validate_domains(DOMAINS_TO_CRAWL)

    existing_df = load_existing_dataset(SETTINGS["output_csv"])

    df = run(DOMAINS_TO_CRAWL, existing_df)

    export(df)
