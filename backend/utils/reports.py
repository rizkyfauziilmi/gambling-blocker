import sqlite3
from pathlib import Path
from urllib.parse import urlparse

DB_PATH: Path = Path(__file__).parent.parent / "reports.db"


def init_db() -> None:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            hostname TEXT NOT NULL,
            gambling_score REAL NOT NULL,
            reporter_ip TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def save_report(url: str, gambling_score: float, reporter_ip: str) -> None:
    hostname: str = urlparse(url).hostname or url
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute(
        "INSERT INTO reports (url, hostname, gambling_score, reporter_ip) VALUES (?, ?, ?, ?)",
        (url, hostname, gambling_score, reporter_ip),
    )
    conn.commit()
    conn.close()


init_db()
