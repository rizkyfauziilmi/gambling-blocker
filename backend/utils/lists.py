import sqlite3
from pathlib import Path

DB_PATH: Path = Path(__file__).parent.parent / "app.db"


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS site_lists (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            hostname TEXT NOT NULL UNIQUE,
            list_type TEXT NOT NULL CHECK(list_type IN ('blacklist', 'whitelist')),
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    conn.commit()
    conn.close()


def add_entry(hostname: str, list_type: str) -> dict[str, object] | None:
    init_db()
    conn = _conn()
    try:
        cur = conn.execute(
            "INSERT INTO site_lists (hostname, list_type) VALUES (?, ?)",
            (hostname, list_type),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM site_lists WHERE id = ?", (cur.lastrowid,)
        ).fetchone()
        conn.close()
        return dict(row) if row else None
    except sqlite3.IntegrityError:
        conn.close()
        return None


def remove_entry(entry_id: int) -> bool:
    init_db()
    conn = _conn()
    cur = conn.execute("DELETE FROM site_lists WHERE id = ?", (entry_id,))
    conn.commit()
    conn.close()
    return cur.rowcount > 0


def get_entries(list_type: str) -> list[dict[str, object]]:
    init_db()
    conn = _conn()
    rows = conn.execute(
        "SELECT * FROM site_lists WHERE list_type = ? ORDER BY created_at DESC",
        (list_type,),
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def check_hostname(hostname: str) -> str | None:
    init_db()
    conn = _conn()
    row = conn.execute(
        "SELECT list_type FROM site_lists WHERE hostname = ?", (hostname,)
    ).fetchone()
    conn.close()
    return row["list_type"] if row else None
