import csv
import json
import logging
import queue as _queue
import re
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
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
    "delay_seconds": 0.5,
    "request_timeout": 10,
    "respect_robots_txt": False,
    "max_workers": 20,
    "output_csv": "dataset_crawl.csv",
    "output_json": "dataset_crawl.json",
    "log_file": "crawler.log",
    "checkpoint_file": "checkpoint.json",  # Layer 2: domain selesai
    "domain_timeout": 300,  # Layer 3: detik sebelum domain dianggap stuck
    "flush_every_n": 10,  # Layer 1: flush tiap N record
    "flush_every_s": 30.0,  # Layer 1: atau tiap N detik
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
#  Semua log ke file — stdout hanya milik tqdm.
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    filename=SETTINGS["log_file"],
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
_logger = logging.getLogger("crawler")

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def log(msg: str = "") -> None:
    """Tulis satu baris ke log file (thread-safe via logging internal lock)."""
    _logger.info(_ANSI_RE.sub("", msg))


def log_block(*lines: str) -> None:
    """Tulis beberapa baris ke log file secara atomik (satu logging call)."""
    _logger.info("\n".join(_ANSI_RE.sub("", ln) for ln in lines))


# ══════════════════════════════════════════════════════════════════════════════
#  TQDM THREAD SAFETY
#  Set sekali di level modul, sebelum ThreadPoolExecutor dibuat.
# ══════════════════════════════════════════════════════════════════════════════

tqdm.set_lock(threading.RLock())

# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 1 — WRITER THREAD
#  Satu thread khusus yang drain result_queue dan append ke CSV secara berkala.
#  Crawler thread hanya perlu result_queue.put(record) — tidak ada lock di sisi mereka.
# ══════════════════════════════════════════════════════════════════════════════

_CSV_FIELDNAMES = ["Webpage_id", "Domain", "Url", "Tag", "Crawled_at"]


class WriterThread(threading.Thread):
    """
    Dedicated CSV writer thread.

    Alur:
      crawler thread -> result_queue.put(record)
                              |
      WriterThread  -> _buffer -> flush (append ke CSV) tiap N record atau T detik

    Keuntungan:
      - Tidak ada file I/O di crawler thread -> tidak ada bottleneck lock.
      - Data tersimpan ke disk secara berkala -> aman saat crash.
    """

    def __init__(
        self,
        result_queue: _queue.Queue,
        csv_path: str,
        flush_every_n: int = 10,
        flush_every_s: float = 30.0,
    ) -> None:
        super().__init__(daemon=True, name="csv-writer")
        self.q = result_queue
        self.csv_path = Path(csv_path)
        self.flush_every_n = flush_every_n
        self.flush_every_s = flush_every_s
        self._stop_event = threading.Event()
        self._buffer: list[dict] = []
        self._last_flush = time.monotonic()
        self.total_written: int = 0  # dibaca setelah join()

    # ── Main loop ─────────────────────────────────────────────────────────────

    def run(self) -> None:
        while not self._stop_event.is_set() or not self.q.empty():
            # Ambil record dari queue; timeout 1s agar loop bisa cek stop_event
            try:
                record = self.q.get(timeout=1.0)
                self._buffer.append(record)
                self.q.task_done()
            except _queue.Empty:
                pass

            # Flush jika buffer cukup besar ATAU sudah terlalu lama tidak flush
            elapsed = time.monotonic() - self._last_flush
            if len(self._buffer) >= self.flush_every_n or elapsed >= self.flush_every_s:
                self._flush()

        self._flush()  # drain sisa buffer sebelum thread mati

    # ── Flush ke disk ─────────────────────────────────────────────────────────

    def _flush(self) -> None:
        if not self._buffer:
            return

        write_header = not self.csv_path.exists() or self.csv_path.stat().st_size == 0

        # Append-only: tidak perlu tmp file karena tidak ada overwrite
        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=_CSV_FIELDNAMES)
            if write_header:
                writer.writeheader()
            writer.writerows(self._buffer)

        self.total_written += len(self._buffer)
        log(
            f"[Writer] Flushed {len(self._buffer)} records "
            f"(total tersimpan: {self.total_written}) -> {self.csv_path}"
        )
        self._buffer.clear()
        self._last_flush = time.monotonic()

    # ── Sinyal stop ───────────────────────────────────────────────────────────

    def stop(self) -> None:
        """Beri tahu thread untuk berhenti setelah queue kosong."""
        self._stop_event.set()


# ══════════════════════════════════════════════════════════════════════════════
#  LAYER 2 — DOMAIN CHECKPOINT
#  Track domain yang sudah selesai penuh di checkpoint.json.
#  Saat resume, domain yang ada di checkpoint di-skip sepenuhnya.
# ══════════════════════════════════════════════════════════════════════════════

_checkpoint_lock = threading.Lock()


def load_checkpoint() -> set[str]:
    """
    Baca checkpoint.json dan kembalikan set netloc yang sudah selesai.
    Kembalikan set kosong jika file belum ada.
    """
    path = Path(SETTINGS["checkpoint_file"])
    if not path.exists():
        return set()

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    completed: set[str] = set(data.get("completed_domains", []))
    log(f"[Checkpoint] {len(completed)} domain sudah selesai dari run sebelumnya.")
    return completed


def mark_domain_done(netloc: str) -> None:
    """
    Tandai netloc sebagai selesai di checkpoint.json.
    Thread-safe via _checkpoint_lock.
    Atomic write: tulis ke .tmp dulu, lalu rename — tidak ada window corrupt.
    """
    with _checkpoint_lock:
        path = Path(SETTINGS["checkpoint_file"])
        data: dict = {"completed_domains": [], "last_updated": ""}

        if path.exists():
            with open(path, encoding="utf-8") as f:
                data = json.load(f)

        if netloc not in data["completed_domains"]:
            data["completed_domains"].append(netloc)

        data["last_updated"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        tmp.replace(path)  # atomic pada semua OS modern

    log(f"[Checkpoint] Marked done: {netloc}")


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
    """Lempar ValueError jika ada netloc duplikat di DOMAINS_TO_CRAWL."""
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
            + "\nHapus salah satu sebelum menjalankan crawler."
        )


# ══════════════════════════════════════════════════════════════════════════════
#  DATASET  (baca CSV untuk resume)
# ══════════════════════════════════════════════════════════════════════════════


def load_existing_dataset(csv_path: str) -> pd.DataFrame:
    """
    Baca CSV yang sudah ada untuk menentukan:
      - URL mana yang sudah dikunjungi (visited_map)
      - ID terakhir (untuk AtomicCounter)
    Kembalikan DataFrame kosong jika belum ada file.
    """
    path = Path(csv_path)
    if not path.exists():
        log("[~] Tidak ada dataset lama. Mulai dari awal.")
        return pd.DataFrame(columns=_CSV_FIELDNAMES)

    df = pd.read_csv(path)

    before = len(df)
    df = df.drop_duplicates(subset=["Url"], keep="first")
    dupes = before - len(df)
    if dupes:
        log(f"[!] {dupes} baris duplikat dihapus dari dataset lama.")

    log(f"[Resume] Dataset lama: {len(df)} baris dari {df['Domain'].nunique()} domain.")
    return df


def get_visited_urls_per_domain(df: pd.DataFrame) -> dict[str, set[str]]:
    """Bangun mapping netloc -> set(url) dari dataset yang sudah ada."""
    if df.empty:
        return {}
    mapping: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        mapping.setdefault(str(row["Domain"]), set()).add(str(row["Url"]))
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
    """Hapus fragment, query, trailing slash; lowercase."""
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/").lower()


def is_root_domain(url: str) -> bool:
    return urlparse(url).path in ("", "/")


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
    links: list[str] = []
    for a in soup.find_all("a", href=True):
        full_url = normalize_url(urljoin(base_url, a["href"].strip()))
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
    return content_type.lower().split(";")[0].strip() in XML_CONTENT_TYPES


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
                log(f"[SKIP XML] {url}")
                return None
            return resp.text
        else:
            log(f"[HTTP {resp.status_code}] {url}")
            return None
    except requests.Timeout:
        log(f"[TIMEOUT] {url}")
        return None
    except requests.ConnectionError:
        log(f"[CONN ERR] {url}")
        return None
    except requests.RequestException as e:
        log(f"[REQ ERR] {url}: {e}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
#  BFS DOMAIN CRAWLER
# ══════════════════════════════════════════════════════════════════════════════

# Domain dengan halaman baru di bawah ini dianggap gagal dan tidak disimpan
_MIN_PAGES_REQUIRED = 1


def crawl_domain(
    start_url: str,
    category: str,
    already_visited: set[str],
    counter: AtomicCounter,
    worker_index: int,
    pos_queue: _queue.Queue,  # pool posisi tqdm — diambil sebelum buka bar
    result_queue: _queue.Queue,  # Layer 1 — output ke WriterThread
) -> int:
    """
    BFS crawler untuk satu domain. Dijalankan sebagai satu thread.

    Strategi streaming dengan min-pages guard:
      - Record ditahan di local_buffer sampai mencapai _MIN_PAGES_REQUIRED.
      - Setelah threshold tercapai, switch ke mode streaming langsung ke result_queue.
      - Jika domain selesai dengan total < _MIN_PAGES_REQUIRED, buffer dibuang
        dan tidak ada record yang masuk result_queue maupun CSV.

    Return: jumlah record yang dikirim ke result_queue (0 jika domain di-skip).
    """
    base_domain = urlparse(start_url).netloc
    max_pages = SETTINGS["max_pages_per_domain"]

    already_count = len(already_visited)
    remaining_quota: int | None = (max_pages - already_count) if max_pages else None

    log_block(
        "",
        f"{'=' * 60}",
        f"  [W{worker_index}] Crawling : {start_url}",
        f"  Domain         : {base_domain}",
        f"  Sudah di-crawl : {already_count} halaman",
        f"  Sisa kuota     : {remaining_quota if remaining_quota is not None else 'unlimited'}",
        f"{'=' * 60}",
    )

    if remaining_quota is not None and remaining_quota <= 0:
        log(f"[W{worker_index}] Kuota habis untuk {base_domain}. Lewati.")
        return 0

    visited: set[str] = set(already_visited)
    bfs_queue: deque[str] = deque([normalize_url(start_url)])
    queued: set[str] = set(bfs_queue)

    # Buffer lokal: tahan record sampai _MIN_PAGES_REQUIRED terpenuhi.
    # Setelah itu switch ke streaming langsung ke result_queue.
    local_buffer: list[dict] = []
    streaming: bool = False  # True setelah buffer pertama kali di-flush ke result_queue
    sent_count: int = 0  # jumlah record yang sudah masuk result_queue

    # ── Ambil slot posisi tqdm dari pool ──────────────────────────────────────
    # Blok di sini sampai ada slot kosong; dijamin ada karena pool = max_workers
    bar_pos: int = pos_queue.get()

    try:
        with tqdm(
            desc=f"  [W{worker_index:>2}] {base_domain:<30}",
            unit=" pg",
            colour="cyan",
            position=bar_pos,
            leave=False,  # hapus bar saat selesai -> slot posisi bebas dipakai ulang
            dynamic_ncols=True,
            miniters=0,
            mininterval=0.1,
        ) as pbar:
            while bfs_queue:
                # Hitung total halaman yang sudah ditangani (dikirim + di buffer)
                current_total = sent_count + len(local_buffer)
                if remaining_quota is not None and current_total >= remaining_quota:
                    log(
                        f"[W{worker_index}] Kuota {remaining_quota} tercapai untuk {base_domain}."
                    )
                    break

                current_url = bfs_queue.popleft()

                if current_url in visited:
                    continue
                visited.add(current_url)

                is_root = is_root_domain(current_url)

                # Fetch hanya jika pipeline belum cukup untuk memenuhi kuota
                urls_in_pipeline = current_total + len(bfs_queue)
                needs_fetch = is_root or (
                    remaining_quota is None or urls_in_pipeline < remaining_quota
                )

                if needs_fetch:
                    html = fetch(current_url)
                    time.sleep(SETTINGS["delay_seconds"])

                    if html is None:
                        continue  # fetch gagal -> jangan rekam URL ini

                    for link in extract_links(html, current_url, base_domain):
                        if link not in queued:
                            bfs_queue.append(link)
                            queued.add(link)

                if is_root:
                    continue  # root domain: seed saja, tidak direkam

                record: dict = {
                    "Webpage_id": counter.next(),
                    "Domain": base_domain,
                    "Url": current_url,
                    "Tag": category,
                    "Crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                if streaming:
                    # Mode streaming: langsung kirim ke WriterThread
                    result_queue.put(record)
                    sent_count += 1
                else:
                    local_buffer.append(record)
                    if len(local_buffer) >= _MIN_PAGES_REQUIRED:
                        # Threshold terpenuhi: flush buffer ke result_queue & switch mode
                        for r in local_buffer:
                            result_queue.put(r)
                        sent_count += len(local_buffer)
                        local_buffer.clear()
                        streaming = True

                mode = "fetch" if needs_fetch else "drain"
                pbar.update(1)
                pbar.set_postfix(
                    queue=len(bfs_queue),
                    new=sent_count + len(local_buffer),
                    total=already_count + sent_count + len(local_buffer),
                    mode=mode,
                )

    finally:
        # Kembalikan slot posisi ke pool — harus ada di finally agar tidak leak
        pos_queue.put(bar_pos)

    # ── Evaluasi hasil setelah BFS selesai ────────────────────────────────────

    total_new = sent_count + len(local_buffer)

    if total_new < _MIN_PAGES_REQUIRED:
        # Domain tidak punya cukup halaman — buang buffer, tidak ada yang dikirim
        log(
            f"[SKIP] {base_domain}: hanya {total_new} halaman "
            f"(min {_MIN_PAGES_REQUIRED}). Record tidak disimpan."
        )
        return 0  # local_buffer otomatis di-GC; tidak ada yang masuk result_queue

    # Domain punya cukup halaman — flush sisa buffer jika masih ada.
    # Terjadi jika domain selesai tepat di angka _MIN_PAGES_REQUIRED tanpa switch streaming.
    if local_buffer:
        for r in local_buffer:
            result_queue.put(r)
        sent_count += len(local_buffer)
        local_buffer.clear()

    log(f"[W{worker_index}] Selesai: {sent_count} halaman baru dari {base_domain}.")
    return sent_count


# ══════════════════════════════════════════════════════════════════════════════
#  MULTI-DOMAIN RUNNER
# ══════════════════════════════════════════════════════════════════════════════


def run(domains: list[tuple[str, str]], existing_df: pd.DataFrame) -> None:
    """
    Submit setiap domain sebagai task ke ThreadPoolExecutor.

    Layer 1 (WriterThread)  : record di-stream ke CSV secara berkala.
    Layer 2 (Checkpoint)    : domain yang sudah selesai di-skip saat resume.
    Layer 3 (Domain timeout): domain yang stuck di-cancel setelah N detik.
    """
    visited_map = get_visited_urls_per_domain(existing_df)
    completed_domains = load_checkpoint()  # Layer 2: baca domain yang sudah done

    id_start = int(existing_df["Webpage_id"].max()) + 1 if not existing_df.empty else 1
    counter = AtomicCounter(start=id_start)

    configured: int = SETTINGS.get("max_workers", 5)
    max_workers = min(configured, len(domains)) if domains else 1

    # ── Layer 1: start WriterThread sebelum executor ──────────────────────────
    result_queue: _queue.Queue = _queue.Queue()
    writer = WriterThread(
        result_queue=result_queue,
        csv_path=SETTINGS["output_csv"],
        flush_every_n=SETTINGS["flush_every_n"],
        flush_every_s=SETTINGS["flush_every_s"],
    )
    writer.start()
    log(f"[Writer] Thread started -> {SETTINGS['output_csv']}")

    # ── Pool posisi tqdm (0..max_workers-1) ───────────────────────────────────
    pos_queue: _queue.Queue = _queue.Queue()
    for i in range(max_workers):
        pos_queue.put(i)

    log(f"[Run] {max_workers} worker(s) untuk {len(domains)} domain...")

    domain_timeout: int = SETTINGS.get("domain_timeout", 300)
    future_to_meta: dict = {}

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, (url, category) in enumerate(domains):
                netloc = urlparse(url).netloc

                # Layer 2: skip domain yang sudah selesai sepenuhnya
                if netloc in completed_domains:
                    log(f"[SKIP] {netloc} sudah di checkpoint. Lewati.")
                    continue

                future = executor.submit(
                    crawl_domain,
                    start_url=url,
                    category=category,
                    already_visited=visited_map.get(netloc, set()),
                    counter=counter,
                    worker_index=idx,
                    pos_queue=pos_queue,
                    result_queue=result_queue,
                )
                future_to_meta[future] = (url, idx, netloc)

            for future in as_completed(future_to_meta):
                url, idx, netloc = future_to_meta[future]
                try:
                    # Layer 3: batas waktu per domain — anti-stuck
                    count: int = future.result(timeout=domain_timeout)

                    if count > 0:
                        mark_domain_done(netloc)  # Layer 2: tandai selesai
                        log(f"[OK] {netloc}: {count} halaman tersimpan.")
                    else:
                        log(f"[~] {netloc}: 0 halaman (tidak di-checkpoint).")

                except FutureTimeoutError:
                    log(
                        f"[TIMEOUT] {url} tidak selesai dalam {domain_timeout}s. "
                        f"Thread dibiarkan mati secara alami."
                    )
                    future.cancel()

                except Exception as exc:
                    log(f"[ERR] Worker {idx} ({url}): {exc}")

    finally:
        # Selalu stop writer — bahkan saat KeyboardInterrupt atau exception tak terduga
        log("[Writer] Menunggu flush terakhir...")
        writer.stop()
        writer.join(timeout=15)
        log(f"[Writer] Selesai. Total record tersimpan: {writer.total_written}.")


# ══════════════════════════════════════════════════════════════════════════════
#  EXPORT  (baca CSV -> JSON + statistik)
# ══════════════════════════════════════════════════════════════════════════════


def export() -> None:
    """
    Baca CSV hasil crawl (ditulis WriterThread), export ke JSON, cetak statistik.
    Dipanggil setelah run() selesai dan writer.join() sudah kembali.
    """
    path = Path(SETTINGS["output_csv"])
    if not path.exists() or path.stat().st_size == 0:
        log("[!] Dataset kosong, tidak ada yang diekspor.")
        return

    df = pd.read_csv(path)
    df = df.sort_values("Webpage_id").reset_index(drop=True)

    df.to_json(SETTINGS["output_json"], orient="records", indent=2)
    log(f"[Export] JSON -> {SETTINGS['output_json']}  ({len(df)} baris)")

    total = max(len(df), 1)
    sep = "-" * 60
    log_block(
        "",
        sep,
        "  Statistik akhir:",
        f"  Total halaman crawled : {len(df)}",
        f"  Jumlah domain         : {df['Domain'].nunique()}",
        "  Distribusi tag:",
        *[
            f"    {tag:<15} {count:>4}  {'#' * (count * 20 // total)}"
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
        "=" * 60,
        "  Multi-Domain Web Crawler & Dataset Builder  [Multithreaded]",
        f"  Domains : {len(DOMAINS_TO_CRAWL)}",
        f"  Workers : {SETTINGS['max_workers']}",
        "=" * 60,
    )

    validate_domains(DOMAINS_TO_CRAWL)

    existing_df = load_existing_dataset(SETTINGS["output_csv"])

    run(DOMAINS_TO_CRAWL, existing_df)

    export()
