import queue as _queue
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from concurrent.futures import TimeoutError as FutureTimeoutError
from datetime import datetime
from urllib.parse import urlparse

import pandas as pd
from tqdm import tqdm

from crawler.fetcher import fetch
from crawler.logger import crawler_log, crawler_log_block
from crawler.storage import (
    WriterThread,
    get_visited_urls_per_domain,
    load_checkpoint,
    mark_domain_done,
)
from settings.crawler import CRAWLER_SETTINGS
from utils.crawler import AtomicCounter, extract_links, is_root_domain, normalize_url

tqdm.set_lock(threading.RLock())

# Domain yang menghasilkan halaman baru di bawah nilai ini akan diabaikan sepenuhnya.
_MIN_PAGES_REQUIRED = 1


def crawl_domain(
    start_url: str,
    category: str,
    already_visited: set[str],
    counter: AtomicCounter,
    worker_index: int,
    pos_queue: _queue.Queue,
    result_queue: _queue.Queue,
) -> int:
    """Crawl satu domain menggunakan BFS dan masukkan record yang ditemukan ke queue.

    Record ditahan di buffer lokal sampai ``_MIN_PAGES_REQUIRED`` halaman
    terkonfirmasi. Setelah threshold terpenuhi, crawler beralih ke mode
    streaming dan mengirim record langsung ke ``result_queue``. Jika domain
    selesai di bawah threshold, buffer dibuang tanpa menghasilkan output apapun.

    URL root (``/`` atau path kosong) hanya digunakan sebagai seed untuk
    penemuan link dan tidak pernah disimpan sebagai record.

    Args:
        start_url: URL awal (seed) untuk domain ini.
        category: Tag/label yang diberikan ke semua halaman dari domain ini.
        already_visited: URL yang sudah di-crawl pada run sebelumnya.
        counter: ``AtomicCounter`` bersama untuk menetapkan nilai ``Webpage_id``.
        worker_index: Indeks worker ini, digunakan untuk logging dan label tqdm.
        pos_queue: Pool posisi bar tqdm; satu slot diambil saat masuk dan
            dikembalikan saat keluar agar posisi bisa didaur ulang antar domain.
        result_queue: Queue output yang dikonsumsi oleh :class:`WriterThread`.

    Returns:
        Jumlah record baru yang dikirim ke ``result_queue`` (0 jika domain
        dilewati atau tidak memenuhi threshold halaman minimum).
    """
    base_domain = urlparse(start_url).netloc
    max_pages = CRAWLER_SETTINGS["max_pages_per_domain"]

    already_count = len(already_visited)
    remaining_quota: int | None = (max_pages - already_count) if max_pages else None

    crawler_log_block(
        "",
        f"{'=' * 60}",
        f"  [W{worker_index}] Crawling : {start_url}",
        f"  Domain          : {base_domain}",
        f"  Sudah dikunjungi: {already_count} halaman",
        f"  Sisa kuota      : {remaining_quota if remaining_quota is not None else 'unlimited'}",
        f"{'=' * 60}",
    )

    if remaining_quota is not None and remaining_quota <= 0:
        crawler_log(f"[W{worker_index}] Kuota habis untuk {base_domain}. Dilewati.")
        return 0

    visited: set[str] = set(already_visited)
    bfs_queue: deque[str] = deque([normalize_url(start_url)])
    queued: set[str] = set(bfs_queue)

    local_buffer: list[dict] = []
    streaming: bool = False  # True setelah buffer pertama kali di-flush ke result_queue
    sent_count: int = 0

    # Blok di sini sampai slot posisi tqdm tersedia.
    bar_pos: int = pos_queue.get()

    try:
        with tqdm(
            desc=f"  [W{worker_index:>2}] {base_domain:<30}",
            unit=" pg",
            colour="cyan",
            position=bar_pos,
            leave=False,
            dynamic_ncols=True,
            miniters=0,
            mininterval=0.1,
        ) as pbar:
            while bfs_queue:
                current_total = sent_count + len(local_buffer)
                if remaining_quota is not None and current_total >= remaining_quota:
                    crawler_log(
                        f"[W{worker_index}] Kuota {remaining_quota} tercapai untuk {base_domain}."
                    )
                    break

                current_url = bfs_queue.popleft()
                if current_url in visited:
                    continue
                visited.add(current_url)

                is_root = is_root_domain(current_url)

                # Lewati fetch jika pipeline sudah cukup untuk memenuhi kuota;
                # URL root tetap di-fetch untuk penemuan link.
                urls_in_pipeline = current_total + len(bfs_queue)
                needs_fetch = is_root or (
                    remaining_quota is None or urls_in_pipeline < remaining_quota
                )

                if needs_fetch:
                    html = fetch(current_url)
                    time.sleep(CRAWLER_SETTINGS["delay_seconds"])

                    if html is None:
                        continue

                    for link in extract_links(html, current_url, base_domain):
                        if link not in queued:
                            bfs_queue.append(link)
                            queued.add(link)

                if is_root:
                    continue  # root hanya sebagai seed, tidak direkam

                record: dict = {
                    "Webpage_id": counter.next(),
                    "Domain": base_domain,
                    "Url": current_url,
                    "Tag": category,
                    "Crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }

                if streaming:
                    result_queue.put(record)
                    sent_count += 1
                else:
                    local_buffer.append(record)
                    if len(local_buffer) >= _MIN_PAGES_REQUIRED:
                        for r in local_buffer:
                            result_queue.put(r)
                        sent_count += len(local_buffer)
                        local_buffer.clear()
                        streaming = True

                pbar.update(1)
                pbar.set_postfix(
                    queue=len(bfs_queue),
                    new=sent_count + len(local_buffer),
                    total=already_count + sent_count + len(local_buffer),
                    mode="fetch" if needs_fetch else "drain",
                )

    finally:
        pos_queue.put(bar_pos)  # selalu kembalikan slot, bahkan saat exception

    total_new = sent_count + len(local_buffer)

    if total_new < _MIN_PAGES_REQUIRED:
        crawler_log(
            f"[SKIP] {base_domain}: hanya {total_new} halaman ditemukan "
            f"(minimum: {_MIN_PAGES_REQUIRED}). Record tidak disimpan."
        )
        return 0

    # Flush sisa buffer jika masih ada — terjadi ketika domain selesai tepat
    # di angka threshold sebelum mode streaming sempat aktif.
    if local_buffer:
        for r in local_buffer:
            result_queue.put(r)
        sent_count += len(local_buffer)
        local_buffer.clear()

    crawler_log(
        f"[W{worker_index}] Selesai: {sent_count} halaman baru dari {base_domain}."
    )
    return sent_count


# ---------------------------------------------------------------------------
# Runner multi-domain
# ---------------------------------------------------------------------------


def run_crawler(domains: list[tuple[str, str]], existing_df: pd.DataFrame) -> None:
    """Submit semua domain ke thread pool dan koordinasikan tiga lapisan keamanan.

    Args:
        domains: Daftar pasangan ``(url, category)`` yang akan di-crawl.
        existing_df: DataFrame dari run sebelumnya untuk melewati URL yang sudah dikunjungi.
    """
    visited_map = get_visited_urls_per_domain(existing_df)
    completed_domains = load_checkpoint()

    id_start = (
        int(existing_df["Webpage_id"].max().item()) + 1 if not existing_df.empty else 1
    )
    counter = AtomicCounter(start=id_start)

    configured: int = CRAWLER_SETTINGS.get("max_workers", 5)
    max_workers = min(configured, len(domains)) if domains else 1

    # Layer 1: jalankan WriterThread sebelum pekerjaan apapun di-submit.
    result_queue: _queue.Queue = _queue.Queue()
    writer = WriterThread(
        result_queue=result_queue,
        csv_path=CRAWLER_SETTINGS["output_csv"],
        flush_every_n=CRAWLER_SETTINGS["flush_every_n"],
        flush_every_s=CRAWLER_SETTINGS["flush_every_s"],
    )
    writer.start()
    crawler_log(f"[Writer] Thread dimulai -> {CRAWLER_SETTINGS['output_csv']}")

    # Daur ulang posisi bar tqdm antar worker (posisi 0..max_workers-1).
    pos_queue: _queue.Queue = _queue.Queue()
    for i in range(max_workers):
        pos_queue.put(i)

    crawler_log(f"[Run] {max_workers} worker untuk {len(domains)} domain...")

    domain_timeout: int = CRAWLER_SETTINGS.get("domain_timeout", 300)
    future_to_meta: dict = {}

    try:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            for idx, (url, category) in enumerate(domains):
                netloc = urlparse(url).netloc

                # Layer 2: lewati domain yang sudah selesai pada run sebelumnya.
                if netloc in completed_domains:
                    crawler_log(f"[SKIP] {netloc} sudah ada di checkpoint.")
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
                    # Layer 3: terapkan batas waktu per domain untuk mencegah worker macet.
                    count: int = future.result(timeout=domain_timeout)

                    if count > 0:
                        mark_domain_done(netloc)
                        crawler_log(f"[OK] {netloc}: {count} halaman disimpan.")
                    else:
                        crawler_log(f"[~] {netloc}: 0 halaman (tidak di-checkpoint).")

                except FutureTimeoutError:
                    crawler_log(
                        f"[TIMEOUT] {url} tidak selesai dalam {domain_timeout} detik. "
                        f"Thread dibiarkan mati secara alami."
                    )
                    future.cancel()

                except Exception as exc:
                    crawler_log(f"[ERR] Worker {idx} ({url}): {exc}")

    finally:
        # Selalu hentikan writer — mencakup KeyboardInterrupt dan exception tak terduga.
        crawler_log("[Writer] Menunggu flush terakhir...")
        writer.stop()
        writer.join(timeout=15)
        crawler_log(
            f"[Writer] Selesai. Total record tersimpan: {writer.total_written}."
        )
