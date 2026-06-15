from __future__ import annotations

# ruff: noqa: E501
import hashlib
import secrets
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DB_PATH: Path = Path(__file__).parent.parent / "app.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_tables() -> None:
    conn = _conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS partner_accounts (
            extension_id TEXT PRIMARY KEY,
            partner_email TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            password_salt TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now')),
            stale_alerted_at TEXT
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
    try:
        conn.execute("ALTER TABLE partner_accounts ADD COLUMN stale_alerted_at TEXT")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600000)
    return h.hex(), salt


def setup_partner(extension_id: str, partner_email: str) -> dict:
    password = secrets.token_urlsafe(12)
    pw_hash, salt = _hash_password(password)
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """INSERT OR REPLACE INTO partner_accounts
           (extension_id, partner_email, password_hash, password_salt, created_at, updated_at)
           VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM partner_accounts WHERE extension_id = ?), ?), ?)""",
        (extension_id, partner_email, pw_hash, salt, extension_id, now, now),
    )
    conn.commit()
    conn.close()
    return {"password": password, "password_hash": pw_hash, "password_salt": salt}


def restore_partner(extension_id: str, password_hash: str, password_salt: str) -> None:
    conn = _conn()
    conn.execute(
        "UPDATE partner_accounts SET password_hash = ?, password_salt = ? WHERE extension_id = ?",
        (password_hash, password_salt, extension_id),
    )
    conn.commit()
    conn.close()


def delete_partner(extension_id: str) -> None:
    conn = _conn()
    conn.execute(
        "DELETE FROM partner_accounts WHERE extension_id = ?", (extension_id,)
    )
    conn.commit()
    conn.close()


def record_heartbeat(extension_id: str, ip_address: str | None = None) -> None:
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO heartbeats (extension_id, timestamp, ip_address) VALUES (?, ?, ?)",
        (extension_id, now, ip_address),
    )
    conn.execute(
        "UPDATE partner_accounts SET stale_alerted_at = NULL WHERE extension_id = ?",
        (extension_id,),
    )
    conn.commit()
    conn.close()


def log_tamper(extension_id: str, event_type: str, details: str = "") -> None:
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO tamper_logs (extension_id, event_type, details, timestamp) VALUES (?, ?, ?, ?)",
        (extension_id, event_type, details, now),
    )
    conn.commit()
    conn.close()


def mark_stale_alerted(extension_id: str) -> None:
    conn = _conn()
    now = datetime.now(timezone.utc).isoformat()
    conn.execute(
        "UPDATE partner_accounts SET stale_alerted_at = ? WHERE extension_id = ?",
        (now, extension_id),
    )
    conn.commit()
    conn.close()


def get_partner(extension_id: str) -> dict | None:
    conn = _conn()
    row = conn.execute(
        "SELECT * FROM partner_accounts WHERE extension_id = ?", (extension_id,)
    ).fetchone()
    conn.close()
    if row is None:
        return None
    return dict(row)


def get_tamper_count(extension_id: str, hours: int = 1) -> int:
    conn = _conn()
    row = conn.execute(
        """SELECT COUNT(*) FROM tamper_logs
           WHERE extension_id = ? AND event_type = 'extensions_page'
           AND datetime(timestamp) > datetime('now', ?)""",
        (extension_id, f"-{hours} hours"),
    ).fetchone()
    conn.close()
    return row[0] if row else 0


def get_heartbeat_age(extension_id: str) -> int | None:
    conn = _conn()
    row = conn.execute(
        "SELECT MAX(timestamp) FROM heartbeats WHERE extension_id = ?",
        (extension_id,),
    ).fetchone()
    conn.close()
    if row is None or row[0] is None:
        return None
    last = datetime.fromisoformat(row[0])
    now = datetime.now(timezone.utc)
    return int((now - last).total_seconds() // 3600)


def get_stale_extensions(hours: int = 24) -> list[dict]:
    conn = _conn()
    rows = conn.execute(
        """SELECT p.*, MAX(h.timestamp) as last_heartbeat
           FROM partner_accounts p
           LEFT JOIN heartbeats h ON h.extension_id = p.extension_id
           GROUP BY p.extension_id
           HAVING (last_heartbeat IS NULL
               OR datetime(last_heartbeat) < datetime('now', ?))
           AND p.stale_alerted_at IS NULL""",
        (f"-{hours} hours",),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_status(extension_id: str) -> dict:
    partner = get_partner(extension_id)
    if partner is None:
        return {"exists": False}
    age = get_heartbeat_age(extension_id)
    tamper_count = get_tamper_count(extension_id, 1)
    return {
        "exists": True,
        "partner_email": partner["partner_email"],
        "heartbeat_age_hours": age,
        "tamper_count_1h": tamper_count,
    }


init_tables()
