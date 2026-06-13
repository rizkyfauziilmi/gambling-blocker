import sqlite3
from pathlib import Path
from urllib.parse import urlparse

DB_PATH: Path = Path(__file__).parent.parent / "reports.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT NOT NULL,
            hostname TEXT NOT NULL,
            gambling_score REAL NOT NULL,
            reporter_ip TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS partner_accounts (
            extension_id TEXT PRIMARY KEY,
            partner_email TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            password_salt TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE TABLE IF NOT EXISTS heartbeats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extension_id TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            ip_address TEXT,
            FOREIGN KEY (extension_id) REFERENCES partner_accounts(extension_id)
        );
        CREATE TABLE IF NOT EXISTS tamper_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            extension_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            details TEXT,
            timestamp TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (extension_id) REFERENCES partner_accounts(extension_id)
        );
    """)
    conn.commit()
    conn.close()


def save_report(url: str, gambling_score: float, reporter_ip: str) -> None:
    hostname: str = urlparse(url).hostname or url
    conn = _conn()
    conn.execute(
        (
            "INSERT INTO reports "
            "(url, hostname, gambling_score, reporter_ip) "
            "VALUES (?, ?, ?, ?)"
        ),
        (url, hostname, gambling_score, reporter_ip),
    )
    conn.commit()
    conn.close()


def get_all_reports(limit: int = 100, offset: int = 0) -> list[dict[str, object]]:
    init_db()
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM reports ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (limit, offset),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def delete_reports_by_hostname(hostname: str) -> None:
    init_db()
    conn = _conn()
    conn.execute("DELETE FROM reports WHERE hostname = ?", (hostname,))
    conn.commit()
    conn.close()


def delete_report(report_id: int) -> bool:
    init_db()
    conn = _conn()
    cur = conn.execute("DELETE FROM reports WHERE id = ?", (report_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_report_stats() -> dict[str, int]:
    init_db()
    conn = _conn()
    total = conn.execute("SELECT COUNT(*) FROM reports").fetchone()[0]
    today = conn.execute(
        "SELECT COUNT(*) FROM reports WHERE date(created_at) = date('now')"
    ).fetchone()[0]
    hostnames = conn.execute("SELECT COUNT(DISTINCT hostname) FROM reports").fetchone()[
        0
    ]
    conn.close()
    return {"total": total, "today": today, "unique_hostnames": hostnames}


def get_grouped_reports() -> list[dict]:
    init_db()
    conn = _conn()
    rows = conn.execute("""
        SELECT
            hostname,
            COUNT(*) AS report_count,
            ROUND(AVG(gambling_score), 4) AS avg_score,
            MAX(created_at) AS last_reported
        FROM reports
        GROUP BY hostname
        ORDER BY report_count DESC, last_reported DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


init_db()
