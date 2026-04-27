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

init(autoreset=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DOMAINS_TO_CRAWL = [
    ("https://www.fiercepharma.com", "non-gambling"),
    ("https://www.polygon.com", "non-gambling"),
]

SETTINGS = {
    "max_pages_per_domain": 200,  # batas halaman per domain (None = unlimited)
    "delay_seconds": 1.0,  # jeda antar request
    "request_timeout": 15,  # timeout per request (detik)
    "max_retries": 2,  # retry jika request gagal
    "respect_robots_txt": False,  # True = skip URL yang dilarang robots.txt
    "checkpoint_every": 10,  # simpan CSV setiap N halaman baru (0 = nonaktif)
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
#  THREAD-SAFE PRIMITIVES
# ══════════════════════════════════════════════════════════════════════════════

# Satu lock untuk semua operasi print — mencegah output terminal bercampur
_print_lock = threading.Lock()

# Satu lock untuk semua operasi tulis CSV — mencegah korupsi file
_csv_lock = threading.Lock()


def safe_print(*args, **kwargs) -> None:
    """
    Wrapper print yang thread-safe. Semua log dari worker mana pun
    harus melalui fungsi ini agar output terminal tetap bersih.
    """
    with _print_lock:
        print(*args, **kwargs)


# ──────────────────────────────────────────────────────────────────────────────


class AtomicCounter:
    """
    Counter integer yang thread-safe menggunakan threading.Lock().

    Setiap panggilan ke `next()` menjamin nilai yang unik dan berurutan
    meskipun dipanggil secara bersamaan oleh banyak thread.

    Contoh penggunaan:
        counter = AtomicCounter(start=1)
        webpage_id = counter.next()  # → 1
        webpage_id = counter.next()  # → 2  (dari thread lain, aman)
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
#  CHECKPOINT  (baca / tulis dataset)
# ══════════════════════════════════════════════════════════════════════════════


def load_existing_dataset(csv_path: str) -> pd.DataFrame:
    """
    Baca dataset CSV yang sudah ada.
    Kembalikan DataFrame kosong jika file belum ada.
    """
    path = Path(csv_path)
    if not path.exists():
        safe_print(
            f"\n{Fore.YELLOW}[~] Tidak ada dataset lama ditemukan. Mulai dari awal."
        )
        return pd.DataFrame(
            columns=["Webpage_id", "Domain", "Url", "Tag", "Crawled_at"]
        )

    df = pd.read_csv(path)

    before = len(df)
    df = df.drop_duplicates(subset=["Url"], keep="first")
    dupes = before - len(df)
    if dupes:
        safe_print(
            f"  {Fore.YELLOW}[!] {dupes} baris duplikat dihapus dari dataset lama."
        )

    return df


def checkpoint_csv(
    existing_df: pd.DataFrame,
    new_records: list[dict],
    csv_path: str,
) -> None:
    """
    Tulis snapshot sementara ke CSV secara thread-safe.

    Menggunakan `_csv_lock` global — hanya satu worker yang boleh menulis
    dalam satu waktu untuk mencegah korupsi file. Worker lain yang memanggil
    fungsi ini secara bersamaan akan mengantre hingga lock dilepas.

    Dipanggil secara periodik selama crawl agar progress tidak hilang
    jika proses terhenti di tengah.
    """
    if not new_records:
        return

    with _csv_lock:
        snapshot = pd.concat(
            [existing_df, pd.DataFrame(new_records)], ignore_index=True
        )
        snapshot.to_csv(csv_path, index=False)


def get_visited_urls_per_domain(df: pd.DataFrame) -> dict[str, set[str]]:
    """
    Bangun mapping netloc → set(url) dari dataset yang sudah ada.
    Dipakai untuk pre-populate `visited` di BFS sehingga URL lama dilewati.
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
    Fetch satu URL dengan retry logic. Tidak ada perubahan dari versi sekuensial —
    fungsi ini sudah thread-safe karena tidak menyentuh state global.
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
                    safe_print(
                        f"  {Fore.YELLOW}[SKIP XML] {url}  "
                        f"({content_type.split(';')[0].strip()})"
                    )
                    return None
                return resp.text

            elif resp.status_code in RETRYABLE_STATUS:
                wait = int(resp.headers.get("Retry-After", 2**attempt))
                safe_print(
                    f"  {Fore.YELLOW}[{resp.status_code}] Retry {attempt + 1}/{retries} "
                    f"dalam {wait}s: {url}"
                )
                if attempt < retries:
                    time.sleep(wait)
                else:
                    safe_print(
                        f"  {Fore.RED}[SKIP] Menyerah setelah {retries} retry: {url}"
                    )
                    return None

            elif resp.status_code == 403:
                safe_print(f"  {Fore.YELLOW}[403] Forbidden (tidak di-retry): {url}")
                return None

            elif resp.status_code == 404:
                return None

            else:
                safe_print(
                    f"  {Fore.YELLOW}[{resp.status_code}] Tidak ditangani: {url}"
                )
                return None

        except requests.Timeout:
            safe_print(
                f"  {Fore.YELLOW}[TIMEOUT] attempt {attempt + 1}/{retries + 1}: {url}"
            )
            if attempt < retries:
                time.sleep(2**attempt)
            else:
                safe_print(f"  {Fore.RED}[SKIP] Timeout habis: {url}")

        except requests.ConnectionError:
            safe_print(f"  {Fore.RED}[CONN ERR] {url}")
            if attempt < retries:
                time.sleep(2**attempt)

        except requests.RequestException as e:
            safe_print(f"  {Fore.RED}[ERR] {url}: {e}")
            if attempt < retries:
                time.sleep(2**attempt)

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  BFS DOMAIN CRAWLER  (idempotent, dijalankan dalam satu thread)
# ══════════════════════════════════════════════════════════════════════════════


def crawl_domain(
    start_url: str,
    category: str,
    already_visited: set[str],
    counter: AtomicCounter,  # ← shared AtomicCounter (thread-safe)
    existing_df: pd.DataFrame,
    worker_index: int,  # ← posisi baris progress bar di terminal
) -> list[dict]:
    """
    BFS crawler untuk satu domain. Dijalankan sebagai satu thread oleh
    ThreadPoolExecutor. Setiap domain mendapatkan thread terpisah sehingga
    crawling berjalan paralel.

    Parameter baru vs versi sekuensial:
    - `counter`      : AtomicCounter bersama — menggantikan `id_start` lokal.
                       Setiap record mengambil ID unik lewat `counter.next()`.
    - `worker_index` : indeks urut domain (0, 1, 2, ...) yang dipakai sebagai
                       `position` pada tqdm agar progress bar tiap domain
                       ditampilkan pada baris terpisah yang tidak tumpang tindih.
    """
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc
    max_pages = SETTINGS["max_pages_per_domain"]
    checkpoint_every = SETTINGS.get("checkpoint_every", 10)

    already_count = len(already_visited)
    remaining_quota = (max_pages - already_count) if max_pages else None

    # ── Header info domain (safe_print agar tidak campur dengan thread lain) ──
    safe_print(f"\n{Fore.CYAN}{'═' * 60}")
    safe_print(f"  [Worker {worker_index}] Crawling : {Fore.WHITE}{start_url}")
    safe_print(f"  Domain   : {base_domain}")
    safe_print(f"  Sudah di-crawl  : {already_count} halaman (dari dataset lama)")
    if remaining_quota is not None:
        safe_print(f"  Sisa kuota      : {remaining_quota} halaman")
    else:
        safe_print("  Sisa kuota      : unlimited")
    if checkpoint_every:
        safe_print(f"  Checkpoint setiap: {checkpoint_every} halaman baru")
    safe_print(f"{Fore.CYAN}{'═' * 60}")

    if remaining_quota is not None and remaining_quota <= 0:
        safe_print(
            f"  {Fore.YELLOW}[!] Kuota {max_pages} halaman sudah habis untuk domain ini. Lewati."
        )
        return []

    # Pre-populate visited → BFS tidak akan menyentuh URL lama
    visited: set[str] = set(already_visited)
    queue = deque([normalize_url(start_url)])
    records: list[dict] = []
    last_checkpoint = 0

    # ── tqdm dengan `position=worker_index` ──────────────────────────────────
    # Setiap worker mendapatkan baris progress bar sendiri berdasarkan indeks.
    # `leave=True` mempertahankan baris setelah crawl domain selesai.
    # `dynamic_ncols=True` menyesuaikan lebar terminal secara otomatis.
    with tqdm(
        desc=f"  [W{worker_index}] {base_domain}",
        unit=" pages",
        colour="cyan",
        position=worker_index,
        leave=True,
        dynamic_ncols=True,
    ) as pbar:
        while queue:
            new_pages = len(records)
            if remaining_quota is not None and new_pages >= remaining_quota:
                safe_print(
                    f"\n  {Fore.YELLOW}[W{worker_index}] Kuota sisa {remaining_quota} halaman tercapai."
                )
                break

            current_url = queue.popleft()

            if current_url in visited:
                continue
            visited.add(current_url)

            html = fetch(current_url)
            time.sleep(SETTINGS["delay_seconds"])

            if html is None:
                continue

            # Ekstrak link (selalu, termasuk root domain)
            new_links = extract_links(html, current_url, base_domain)
            for link in new_links:
                if link not in visited:
                    queue.append(link)

            # Root domain: gunakan sebagai titik awal saja, jangan simpan
            if is_root_domain(current_url):
                continue

            # ── Ambil ID unik dari AtomicCounter bersama ─────────────────
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
            pbar.update(1)
            pbar.set_postfix(
                {
                    "queue": len(queue),
                    "new": len(records),
                    "total": already_count + len(records),
                }
            )

            # ── Periodic checkpoint (thread-safe via _csv_lock di dalam) ──
            if (
                checkpoint_every
                and (len(records) - last_checkpoint) >= checkpoint_every
            ):
                checkpoint_csv(existing_df, records, SETTINGS["output_csv"])
                last_checkpoint = len(records)
                pbar.set_postfix(
                    {
                        "queue": len(queue),
                        "new": len(records),
                        "total": already_count + len(records),
                        "💾": "saved",
                    }
                )

    status = f"{Fore.GREEN}✓" if records else f"{Fore.YELLOW}–"
    safe_print(
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
    Satu thread per domain berjalan paralel secara penuh.

    Perubahan utama dari versi sekuensial:
    - `AtomicCounter` dibuat sekali di sini dan di-pass ke semua worker.
      Worker tidak lagi menerima `id_start` statis — mereka memanggil
      `counter.next()` untuk setiap halaman sehingga ID selalu unik global.
    - `as_completed()` mengumpulkan hasil dari thread yang selesai lebih
      dulu, bukan berdasarkan urutan submit.
    - Logika idempotency (visited_map) tetap identik dengan versi sekuensial.
    """
    visited_map = get_visited_urls_per_domain(existing_df)

    # AtomicCounter dimulai dari max existing ID + 1 (atau 1 jika dataset kosong)
    id_start = int(existing_df["Webpage_id"].max()) + 1 if not existing_df.empty else 1
    counter = AtomicCounter(start=id_start)

    all_new_records: list[dict] = []
    max_workers = len(domains)  # satu worker per domain

    safe_print(
        f"\n{Fore.CYAN}  Meluncurkan {max_workers} worker thread(s)…{Fore.RESET}"
    )

    # Peta future → (url, worker_index) untuk logging hasil
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
                existing_df=existing_df,
                worker_index=idx,
            )
            future_to_meta[future] = (url, idx)

        # Kumpulkan hasil sesuai urutan selesai (bukan urutan submit)
        for future in as_completed(future_to_meta):
            url, idx = future_to_meta[future]
            try:
                new_records = future.result()
                all_new_records.extend(new_records)
            except Exception as exc:
                safe_print(f"\n{Fore.RED}  [Worker {idx}] Exception untuk {url}: {exc}")

    if all_new_records:
        new_df = pd.DataFrame(all_new_records)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = existing_df.copy()
        safe_print(f"\n{Fore.YELLOW}[~] Tidak ada halaman baru yang di-crawl.")

    return combined_df


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════


def export(df: pd.DataFrame) -> None:
    if df.empty:
        safe_print(f"\n{Fore.RED}[!] Dataset kosong, tidak ada yang diekspor.")
        return

    # Urutkan berdasarkan Webpage_id agar output rapi meski ID diambil out-of-order
    df = df.sort_values("Webpage_id").reset_index(drop=True)

    df.to_csv(SETTINGS["output_csv"], index=False)
    safe_print(f"\n{Fore.GREEN}✓ CSV  → {SETTINGS['output_csv']}  ({len(df)} baris)")

    df.to_json(SETTINGS["output_json"], orient="records", indent=2)
    safe_print(f"{Fore.GREEN}✓ JSON → {SETTINGS['output_json']}")

    preview_cols = ["Webpage_id", "Domain", "Url", "Tag"]
    safe_print(f"\n{'─' * 60}")
    safe_print("  Preview dataset (5 baris terakhir):")
    safe_print(f"{'─' * 60}")
    safe_print(df[preview_cols].tail(5).to_string(index=True))

    safe_print(f"\n{'─' * 60}")
    safe_print("  Statistik:")
    safe_print(f"  Total halaman crawled : {len(df)}")
    safe_print(f"  Jumlah domain         : {df['Domain'].nunique()}")
    safe_print("  Distribusi tag:")
    for tag, count in df["Tag"].value_counts().items():
        bar = "█" * (count * 20 // len(df))
        safe_print(f"    {tag:<15} {count:>4}  {bar}")
    safe_print(f"{'─' * 60}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    safe_print(f"\n{Fore.CYAN}{'═' * 60}")
    safe_print("  Multi-Domain Web Crawler & Dataset Builder  [Multithreaded]")
    safe_print(f"  Domains : {len(DOMAINS_TO_CRAWL)}")
    safe_print(f"  Workers : {len(DOMAINS_TO_CRAWL)}  (1 thread per domain)")
    safe_print(f"{'═' * 60}")

    # 1. Validasi — lempar error jika ada domain duplikat
    validate_domains(DOMAINS_TO_CRAWL)

    # 2. Muat dataset lama (jika ada)
    existing_df = load_existing_dataset(SETTINGS["output_csv"])

    # 3. Crawl paralel (hanya URL yang belum dikunjungi)
    df = run(DOMAINS_TO_CRAWL, existing_df)

    # 4. Ekspor dataset lengkap (lama + baru)
    export(df)
