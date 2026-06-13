from __future__ import annotations

import json
from pathlib import Path

_PATH: Path = Path(__file__).resolve().parent.parent / "settings.json"

DEFAULT: dict = {
    "bypass_text_enabled": True,
    "multipage_enabled": True,
    "debug_logging_enabled": False,
    "cache_ttl_hours": 24,
    "stale_hours": 2,
    "stale_check_interval_minutes": 30,
}


def get() -> dict:
    try:
        with open(_PATH) as f:
            stored = json.load(f)
        return {**DEFAULT, **stored}
    except (FileNotFoundError, json.JSONDecodeError):
        return dict(DEFAULT)


def save(updates: dict) -> dict:
    current = get()
    current.update(updates)
    current = {k: current[k] for k in DEFAULT}
    _PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_PATH, "w") as f:
        json.dump(current, f, indent=2)
    return current
