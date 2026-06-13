from __future__ import annotations

from collections import deque
from datetime import datetime

from .settings import get as get_settings

_logs: deque[dict] = deque(maxlen=1000)
_DEBUG_TAGS: frozenset = frozenset({"SCREENSHOT", "MULTIPAGE", "DBG"})


def log(tag: str, message: str) -> None:
    entry = {"time": datetime.now().isoformat(), "tag": tag, "message": message}
    _logs.append(entry)
    if tag in _DEBUG_TAGS and not get_settings().get("debug_logging_enabled", False):
        return
    print(f"[{tag}] {message}")


def get_logs(tag: str | None = None) -> list[dict]:
    if tag:
        return [e for e in _logs if e["tag"] == tag]
    return list(_logs)


def clear() -> None:
    _logs.clear()
