import os
import logging
import re
import sys
from typing import Optional

from settings.crawler import CRAWLER_SETTINGS


def get_logger(
    name: str, log_file: Optional[str] = None, level=logging.DEBUG
) -> logging.Logger:
    """
    Jika log_file diberikan, log masuk ke file.
    Jika log_file None, log cetak ke terminal (stdout).
    """

    os.makedirs("logs", exist_ok=True)
    logger = logging.getLogger(name)

    if not logger.handlers:
        logger.setLevel(level)

        # Pilih Handler
        if log_file:
            # Mode 'a' untuk menambahkan isi file
            handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        else:
            # Mode cetak ke terminal
            handler = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter(
            fmt="%(asctime)s  %(levelname)-8s  %(name)-10s  %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


_c_logger = get_logger("crawler", log_file=CRAWLER_SETTINGS["log_file"])

# Regex untuk membersihkan kode warna terminal (ANSI)
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")


def _clean(text: str) -> str:
    """Helper internal untuk memastikan input adalah string dan bersih dari ANSI."""
    return _ANSI_RE.sub("", str(text))


def crawler_log(msg: str = "") -> None:
    """
    Menulis satu baris ke file log crawler.
    Thread-safe melalui lock internal dari library logging.
    """
    _c_logger.info(_clean(msg))


def crawler_log_block(*lines: str) -> None:
    """
    Menulis beberapa baris sekaligus secara atomik.
    Berguna agar pesan dari thread yang sama tidak terpisah-pisah di dalam file log.
    """
    # Gabungkan semua baris menjadi satu string panjang dengan newline
    # Menambahkan newline di awal agar blok terlihat jelas pemisahnya
    content = "\n".join(_clean(ln) for ln in lines)
    _c_logger.info(f"\n{content}")
