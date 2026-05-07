import csv
import json
import queue as _queue
import threading
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

from crawler.logger import crawler_log, crawler_log_block
from settings.crawler import CRAWLER_SETTINGS, CSV_FIELDNAMES


class WriterThread(threading.Thread):
    """Thread penulis CSV yang menguras queue bersama di background.

    Crawler thread memanggil ``result_queue.put(record)`` dan langsung
    melanjutkan pekerjaannya; thread ini menangani seluruh I/O file sehingga
    tidak ada lock contention di sisi crawler. Buffer di-flush ke disk setiap
    ``flush_every_n`` record atau setiap ``flush_every_s`` detik, memberikan
    keamanan dari crash tanpa mengorbankan throughput.

    Attributes:
        total_written: Jumlah total record yang telah ditulis ke disk.
            Aman dibaca setelah ``join()`` selesai.
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
        self.total_written: int = 0

    def run(self) -> None:
        """Loop utama: ambil record dari queue dan flush buffer secara berkala."""
        while not self._stop_event.is_set() or not self.q.empty():
            try:
                record = self.q.get(timeout=1.0)
                self._buffer.append(record)
                self.q.task_done()
            except _queue.Empty:
                pass

            elapsed = time.monotonic() - self._last_flush
            if len(self._buffer) >= self.flush_every_n or elapsed >= self.flush_every_s:
                self._flush()

        self._flush()  # kosongkan sisa buffer sebelum thread berhenti

    def _flush(self) -> None:
        """Tambahkan isi buffer ke file CSV lalu bersihkan buffer."""
        if not self._buffer:
            return

        write_header = not self.csv_path.exists() or self.csv_path.stat().st_size == 0

        with open(self.csv_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_FIELDNAMES)
            if write_header:
                writer.writeheader()
            writer.writerows(self._buffer)

        self.total_written += len(self._buffer)
        crawler_log(
            f"[Writer] Flush {len(self._buffer)} record "
            f"(total: {self.total_written}) -> {self.csv_path}"
        )
        self._buffer.clear()
        self._last_flush = time.monotonic()

    def stop(self) -> None:
        """Beri sinyal agar thread berhenti setelah queue kosong."""
        self._stop_event.set()


# ---------------------------------------------------------------------------
# Layer 2 — Checkpoint domain
# ---------------------------------------------------------------------------

_checkpoint_lock = threading.Lock()


def load_checkpoint() -> set[str]:
    """Muat daftar netloc domain yang sudah selesai di-crawl dari file checkpoint.

    Returns:
        Kumpulan string netloc (misalnya ``"example.com"``) yang selesai pada
        run sebelumnya. Mengembalikan set kosong jika file checkpoint belum ada.
    """
    path = Path(CRAWLER_SETTINGS["checkpoint_file"])
    if not path.exists():
        return set()

    with open(path, encoding="utf-8") as f:
        data = json.load(f)

    completed: set[str] = set(data.get("completed_domains", []))
    crawler_log(
        f"[Checkpoint] {len(completed)} domain sudah selesai dari run sebelumnya."
    )
    return completed


def mark_domain_done(netloc: str) -> None:
    """Catat domain sebagai selesai di-crawl ke dalam file checkpoint.

    Menggunakan penulisan atomik (tulis ke ``.tmp`` lalu rename) untuk
    mencegah file checkpoint rusak jika proses dihentikan di tengah penulisan.

    Args:
        netloc: String netloc domain yang akan ditandai selesai.
    """
    with _checkpoint_lock:
        path = Path(CRAWLER_SETTINGS["checkpoint_file"])
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
        tmp.replace(path)  # atomik di semua OS modern

    crawler_log(f"[Checkpoint] Ditandai selesai: {netloc}")


def load_existing_dataset(csv_path: str) -> pd.DataFrame:
    """Baca CSV output yang sudah ada untuk mendukung resume crawl yang terputus.

    Args:
        csv_path: Path ke file CSV hasil run sebelumnya.

    Returns:
        DataFrame yang sudah dideduplikasi berisi record lama, atau DataFrame
        kosong dengan skema kolom yang benar jika file belum ada.
    """
    path = Path(csv_path)
    if not path.exists():
        crawler_log("[~] Dataset lama tidak ditemukan. Mulai dari awal.")
        return pd.DataFrame(columns=CSV_FIELDNAMES)

    df = pd.read_csv(path)

    before = len(df)
    df = df.drop_duplicates(subset=["Url"], keep="first")
    dupes = before - len(df)
    if dupes:
        crawler_log(f"[!] {dupes} baris duplikat dihapus dari dataset lama.")

    crawler_log(
        f"[Resume] Dataset lama: {len(df)} baris dari {df['Domain'].nunique()} domain."
    )
    return df


def get_visited_urls_per_domain(df: pd.DataFrame) -> dict[str, set[str]]:
    """Bangun peta ``netloc -> kumpulan URL yang sudah dikunjungi`` dari dataset.

    Args:
        df: DataFrame yang dikembalikan oleh :func:`load_existing_dataset`.

    Returns:
        Dictionary dengan netloc domain sebagai kunci dan set URL yang sudah
        di-crawl sebagai nilai. Mengembalikan dict kosong jika ``df`` kosong.
    """
    if df.empty:
        return {}
    mapping: dict[str, set[str]] = {}
    for _, row in df.iterrows():
        mapping.setdefault(str(row["Domain"]), set()).add(str(row["Url"]))
    return mapping


def export() -> None:
    """Baca CSV hasil crawl, export ke JSON, dan catat statistik ringkasan.

    Harus dipanggil setelah :func:`run` dan setelah ``writer.join()`` selesai
    untuk memastikan semua record sudah di-flush ke disk.
    """
    path = Path(CRAWLER_SETTINGS["output_csv"])
    if not path.exists() or path.stat().st_size == 0:
        crawler_log("[!] Dataset kosong; tidak ada yang diekspor.")
        return

    df = pd.read_csv(path)
    df = df.sort_values("Webpage_id").reset_index(drop=True)

    df.to_json(CRAWLER_SETTINGS["output_json"], orient="records", indent=2)
    crawler_log(
        f"[Export] JSON -> {CRAWLER_SETTINGS['output_json']}  ({len(df)} baris)"
    )

    total = max(len(df), 1)
    sep = "-" * 60
    crawler_log_block(
        "",
        sep,
        "  Statistik akhir:",
        f"  Total halaman di-crawl : {len(df)}",
        f"  Jumlah domain unik     : {df['Domain'].nunique()}",
        "  Distribusi tag:",
        *[
            f"    {tag:<15} {count:>4}  {'#' * (count * 20 // total)}"
            for tag, count in df["Tag"].value_counts().items()
        ],
        sep,
    )
