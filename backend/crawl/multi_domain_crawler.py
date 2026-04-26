import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse, urljoin, urldefrag
from collections import deque
import pandas as pd
import time
import re
from datetime import datetime
from tqdm import tqdm
from colorama import Fore, init
from pathlib import Path

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
#  VALIDATION
# ══════════════════════════════════════════════════════════════════════════════


def validate_domains(domains: list[tuple[str, str]]) -> None:
    """
    Lempar ValueError jika ada domain duplikat di DOMAINS_TO_CRAWL.
    Normalisasi dengan netloc agar http vs https atau trailing slash tidak lolos.
    """
    seen: dict[str, str] = {}  # netloc -> url asli
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
#  CHECKPOINT  (baca dataset lama)
# ══════════════════════════════════════════════════════════════════════════════


def load_existing_dataset(csv_path: str) -> pd.DataFrame:
    """
    Baca dataset CSV yang sudah ada.
    Kembalikan DataFrame kosong jika file belum ada.
    """
    path = Path(csv_path)
    if not path.exists():
        print(f"\n{Fore.YELLOW}[~] Tidak ada dataset lama ditemukan. Mulai dari awal.")
        return pd.DataFrame(
            columns=["Webpage_id", "Domain", "Url", "Tag", "Crawled_at"]
        )

    df = pd.read_csv(path)

    before = len(df)
    df = df.drop_duplicates(subset=["Url"], keep="first")
    dupes = before - len(df)
    if dupes:
        print(f"  {Fore.YELLOW}[!] {dupes} baris duplikat dihapus dari dataset lama.")

    return df


def checkpoint_csv(
    existing_df: pd.DataFrame, new_records: list[dict], csv_path: str
) -> None:
    """
    Tulis snapshot sementara (existing + new_records_sejauh_ini) ke CSV.
    Dipanggil secara periodik selama crawl agar progress tidak hilang jika terhenti.
    """
    if not new_records:
        return
    snapshot = pd.concat([existing_df, pd.DataFrame(new_records)], ignore_index=True)
    snapshot.to_csv(csv_path, index=False)


def get_visited_urls_per_domain(df: pd.DataFrame) -> dict[str, set[str]]:
    """
    Bangun mapping  netloc → set(url) dari dataset yang sudah ada.
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
    """
    True jika URL adalah root domain — path kosong atau hanya '/'.
    Contoh: https://www.fiercepharma.com  → True
            https://www.fiercepharma.com/article/x → False
    """
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

# Status yang layak di-retry
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

# Content-Type yang mengindikasikan XML — skip karena bukan halaman HTML
XML_CONTENT_TYPES = {
    "application/xml",
    "text/xml",
    "application/rss+xml",
    "application/atom+xml",
}


def is_xml_content_type(content_type: str) -> bool:
    """
    True jika Content-Type response adalah XML/RSS/Atom.
    Menggunakan prefix-match agar parameter seperti '; charset=utf-8' diabaikan.
    """
    ct = content_type.lower().split(";")[0].strip()
    return ct in XML_CONTENT_TYPES


def fetch(url: str, retries: int = SETTINGS["max_retries"]) -> str | None:
    for attempt in range(retries + 1):
        try:
            resp = requests.get(
                url,
                headers=HEADERS,
                timeout=SETTINGS["request_timeout"],
                allow_redirects=True,
            )

            if resp.status_code == 200:
                # ── Skip XML (sitemap, RSS, Atom) — bukan konten halaman ──
                content_type = resp.headers.get("Content-Type", "")
                if is_xml_content_type(content_type):
                    print(
                        f"  {Fore.YELLOW}[SKIP XML] {url}  ({content_type.split(';')[0].strip()})"
                    )
                    return None

                return resp.text

            elif resp.status_code in RETRYABLE_STATUS:
                # Retry dengan backoff, hormati Retry-After jika ada
                wait = int(resp.headers.get("Retry-After", 2**attempt))
                print(
                    f"  {Fore.YELLOW}[{resp.status_code}] Retry {attempt + 1}/{retries} "
                    f"dalam {wait}s: {url}"
                )
                if attempt < retries:
                    time.sleep(wait)
                else:
                    print(f"  {Fore.RED}[SKIP] Menyerah setelah {retries} retry: {url}")
                    return None

            elif resp.status_code in {301, 302}:
                # Dead code karena allow_redirects=True, tapi aman dibiarkan
                return None

            elif resp.status_code == 403:
                print(f"  {Fore.YELLOW}[403] Forbidden (tidak di-retry): {url}")
                return None

            elif resp.status_code == 404:
                # Halaman tidak ada, tidak perlu retry
                return None

            else:
                print(f"  {Fore.YELLOW}[{resp.status_code}] Tidak ditangani: {url}")
                return None

        except requests.Timeout:
            print(
                f"  {Fore.YELLOW}[TIMEOUT] attempt {attempt + 1}/{retries + 1}: {url}"
            )
            if attempt < retries:
                time.sleep(2**attempt)
            else:
                print(f"  {Fore.RED}[SKIP] Timeout habis: {url}")

        except requests.ConnectionError:
            print(f"  {Fore.RED}[CONN ERR] {url}")
            if attempt < retries:
                time.sleep(2**attempt)

        except requests.RequestException as e:
            print(f"  {Fore.RED}[ERR] {url}: {e}")
            if attempt < retries:
                time.sleep(2**attempt)

    return None


# ══════════════════════════════════════════════════════════════════════════════
#  BFS DOMAIN CRAWLER  (idempotent)
# ══════════════════════════════════════════════════════════════════════════════


def crawl_domain(
    start_url: str,
    category: str,
    already_visited: set[str],
    id_start: int,
    existing_df: pd.DataFrame,
) -> list[dict]:
    """
    BFS crawler dengan dukungan resume + periodic checkpoint:
    - `already_visited` : set URL yang sudah di-crawl di sesi sebelumnya.
    - `id_start`        : angka awal Webpage_id agar tidak bentrok dengan data lama.
    - `existing_df`     : DataFrame lama, dipakai untuk menyimpan checkpoint sementara.

    Setiap `checkpoint_every` halaman baru, snapshot (lama + baru_sejauh_ini)
    ditulis ke CSV sehingga progress tidak hilang jika proses terhenti di tengah.
    """
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc
    max_pages = SETTINGS["max_pages_per_domain"]
    checkpoint_every = SETTINGS.get("checkpoint_every", 10)

    # Hitung sisa kuota halaman yang boleh di-crawl
    already_count = len(already_visited)
    remaining_quota = (max_pages - already_count) if max_pages else None

    print(f"\n{Fore.CYAN}{'═' * 60}")
    print(f"  Crawling : {Fore.WHITE}{start_url}")
    print(f"  Domain   : {base_domain}")
    print(f"  Sudah di-crawl  : {already_count} halaman (dari dataset lama)")
    if remaining_quota is not None:
        print(f"  Sisa kuota      : {remaining_quota} halaman")
    else:
        print("  Sisa kuota      : unlimited")
    if checkpoint_every:
        print(f"  Checkpoint setiap: {checkpoint_every} halaman baru")
    print(f"{Fore.CYAN}{'═' * 60}")

    if remaining_quota is not None and remaining_quota <= 0:
        print(
            f"  {Fore.YELLOW}[!] Kuota {max_pages} halaman sudah habis untuk domain ini. "
            "Lewati."
        )
        return []

    # Pre-populate visited dengan URL lama → BFS tidak akan menyentuhnya lagi
    visited: set[str] = set(already_visited)
    queue = deque([normalize_url(start_url)])
    records: list[dict] = []
    webpage_id = id_start
    last_checkpoint = 0  # jumlah records saat checkpoint terakhir

    with tqdm(desc=f"  {base_domain}", unit=" pages", colour="cyan") as pbar:
        while queue:
            new_pages = len(records)
            if remaining_quota is not None and new_pages >= remaining_quota:
                print(
                    f"\n  {Fore.YELLOW}[!] Kuota sisa {remaining_quota} halaman tercapai."
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

            # ── Ekstrak link terlebih dahulu (selalu, termasuk root domain) ──
            new_links = extract_links(html, current_url, base_domain)
            for link in new_links:
                if link not in visited:
                    queue.append(link)

            # ── Root domain: gunakan sebagai titik awal saja, jangan simpan ──
            if is_root_domain(current_url):
                continue

            records.append(
                {
                    "Webpage_id": webpage_id,
                    "Domain": base_domain,
                    "Url": current_url,
                    "Tag": category,
                    "Crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
            )
            webpage_id += 1
            pbar.update(1)
            pbar.set_postfix(
                {
                    "queue": len(queue),
                    "new": len(records),
                    "total": already_count + len(records),
                }
            )

            # ── Periodic checkpoint ─────────────────────────────────
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
    print(
        f"\n  {status} Selesai: {len(records)} halaman baru di-crawl dari {base_domain}"
    )
    return records


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-DOMAIN RUNNER  (idempotent)
# ══════════════════════════════════════════════════════════════════════════════


def run(domains: list[tuple[str, str]], existing_df: pd.DataFrame) -> pd.DataFrame:
    """
    Iterasi semua domain, lanjutkan dari titik terakhir berdasarkan dataset lama.
    Gabungkan hasil baru dengan data lama lalu kembalikan DataFrame lengkap.
    """
    visited_map = get_visited_urls_per_domain(existing_df)

    # ID awal untuk record baru = max existing + 1 (atau 1 jika kosong)
    global_id_start = (
        int(existing_df["Webpage_id"].max()) + 1 if not existing_df.empty else 1
    )

    all_new_records: list[dict] = []

    for url, category in domains:
        netloc = urlparse(url).netloc
        already_visited = visited_map.get(netloc, set())

        new_records = crawl_domain(
            start_url=url,
            category=category,
            already_visited=already_visited,
            id_start=global_id_start,
            existing_df=existing_df,
        )

        global_id_start += len(new_records)
        all_new_records.extend(new_records)

    # Gabungkan data lama + data baru
    if all_new_records:
        new_df = pd.DataFrame(all_new_records)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        combined_df = existing_df.copy()
        print(f"\n{Fore.YELLOW}[~] Tidak ada halaman baru yang di-crawl.")

    return combined_df


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════


def export(df: pd.DataFrame):
    if df.empty:
        print(f"\n{Fore.RED}[!] Dataset kosong, tidak ada yang diekspor.")
        return

    df.to_csv(SETTINGS["output_csv"], index=False)
    print(f"\n{Fore.GREEN}✓ CSV  → {SETTINGS['output_csv']}  ({len(df)} baris)")

    df.to_json(SETTINGS["output_json"], orient="records", indent=2)
    print(f"{Fore.GREEN}✓ JSON → {SETTINGS['output_json']}")

    preview_cols = ["Webpage_id", "Domain", "Url", "Tag"]
    print(f"\n{'─' * 60}")
    print("  Preview dataset (5 baris terakhir):")
    print(f"{'─' * 60}")
    print(df[preview_cols].tail(5).to_string(index=True))

    print(f"\n{'─' * 60}")
    print("  Statistik:")
    print(f"  Total halaman crawled : {len(df)}")
    print(f"  Jumlah domain         : {df['Domain'].nunique()}")
    print("  Distribusi tag:")
    for tag, count in df["Tag"].value_counts().items():
        bar = "█" * (count * 20 // len(df))
        print(f"    {tag:<15} {count:>4}  {bar}")
    print(f"{'─' * 60}\n")


# ══════════════════════════════════════════════════════════════════════════════
#  ENTRY POINT
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print(f"\n{Fore.CYAN}{'═' * 60}")
    print("  Multi-Domain Web Crawler & Dataset Builder")
    print(f"  Domains: {len(DOMAINS_TO_CRAWL)}")
    print(f"{'═' * 60}")

    # 1. Validasi — lempar error jika ada domain duplikat
    validate_domains(DOMAINS_TO_CRAWL)

    # 2. Muat dataset lama (jika ada)
    existing_df = load_existing_dataset(SETTINGS["output_csv"])

    # 3. Crawl (hanya URL yang belum dikunjungi)
    df = run(DOMAINS_TO_CRAWL, existing_df)

    # 4. Ekspor dataset lengkap (lama + baru)
    export(df)
