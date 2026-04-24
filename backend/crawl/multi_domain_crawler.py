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

init(autoreset=True)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

DOMAINS_TO_CRAWL = [("https://www.fiercepharma.com", "non-gambling")]

SETTINGS = {
    "max_pages_per_domain": 200,  # batas halaman per domain (None = unlimited)
    "delay_seconds": 1.0,  # jeda antar request
    "request_timeout": 15,  # timeout per request (detik)
    "max_retries": 2,  # retry jika request gagal
    "respect_robots_txt": False,  # True = skip URL yang dilarang robots.txt
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
    url, _ = urldefrag(url)  # hapus fragment (#section)
    url = url.rstrip("/")  # hapus trailing slash
    return url.lower()


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
                return resp.text
            elif resp.status_code in (301, 302):
                return None  # redirect handled by requests
            elif resp.status_code in (403, 429):
                print(f"  {Fore.YELLOW}[{resp.status_code}] Blocked: {url}")
                return None
            else:
                return None
        except requests.RequestException as e:
            if attempt < retries:
                time.sleep(2**attempt)  # exponential backoff
            else:
                print(f"  {Fore.RED}[ERR] {url}: {e}")
    return None


# ══════════════════════════════════════════════════════════════════════════════
#  BFS DOMAIN CRAWLER
# ══════════════════════════════════════════════════════════════════════════════


def crawl_domain(start_url: str, category: str) -> list[dict]:
    """
    BFS crawler — mulai dari start_url, temukan semua link internal,
    scrape setiap halaman, dan kembalikan list of records.
    """
    parsed_start = urlparse(start_url)
    base_domain = parsed_start.netloc
    max_pages = SETTINGS["max_pages_per_domain"]

    queue = deque([normalize_url(start_url)])
    visited = set()
    records = []
    webpage_id = 1

    print(f"\n{Fore.CYAN}{'═' * 60}")
    print(f"  Crawling: {Fore.WHITE}{start_url}")
    print(f"  Domain  : {base_domain}")
    print(f"  Max pages: {max_pages or 'unlimited'}")
    print(f"{Fore.CYAN}{'═' * 60}")

    with tqdm(desc=f"  {base_domain}", unit=" pages", colour="cyan") as pbar:
        while queue:
            if max_pages and len(visited) >= max_pages:
                print(f"\n  {Fore.YELLOW}[!] Batas {max_pages} halaman tercapai.")
                break

            current_url = queue.popleft()

            if current_url in visited:
                continue
            visited.add(current_url)

            html = fetch(current_url)
            time.sleep(SETTINGS["delay_seconds"])

            if html is None:
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
            pbar.set_postfix({"queue": len(queue), "found": len(records)})

            # ── Temukan link baru ───────────────────────────────────
            new_links = extract_links(html, current_url, base_domain)
            for link in new_links:
                if link not in visited:
                    queue.append(link)

    print(
        f"\n  {Fore.GREEN}✓ Selesai: {len(records)} halaman di-crawl dari {base_domain}"
    )
    return records


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-DOMAIN RUNNER
# ══════════════════════════════════════════════════════════════════════════════


def run(domains: list[tuple[str, str]]) -> pd.DataFrame:
    all_records = []
    id_offset = 0

    for url, category in DOMAINS_TO_CRAWL:
        records = crawl_domain(url, category)

        # Pastikan Webpage_id unik di seluruh dataset
        for r in records:
            r["Webpage_id"] += id_offset
        id_offset += len(records)

        all_records.extend(records)

    df = pd.DataFrame(all_records)
    return df


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT
# ══════════════════════════════════════════════════════════════════════════════


def export(df: pd.DataFrame):
    if df.empty:
        print(f"\n{Fore.RED}[!] Dataset kosong, tidak ada yang diekspor.")
        return

    # CSV
    df.to_csv(SETTINGS["output_csv"], index=False)
    print(f"\n{Fore.GREEN}✓ CSV  → {SETTINGS['output_csv']}  ({len(df)} baris)")

    # JSON
    df.to_json(SETTINGS["output_json"], orient="records", indent=2)
    print(f"{Fore.GREEN}✓ JSON → {SETTINGS['output_json']}")

    # Preview di terminal
    preview_cols = ["Webpage_id", "Domain", "Url", "Tag"]
    print(f"\n{'─' * 60}")
    print("  Preview dataset (5 baris pertama):")
    print(f"{'─' * 60}")
    print(df[preview_cols].head(5).to_string(index=True))

    # Statistik
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

    df = run(DOMAINS_TO_CRAWL)
    export(df)
