import json
import os
import time
from collections import defaultdict

import redis as redis_lib

from .logger import log
from .settings import get as get_settings
from .storage import enrich_screenshot_url

_redis: redis_lib.Redis | None = None
_redis_available: bool = False
_connected: bool = False
_mem_limiter: dict[str, list[float]] = defaultdict(list)


def connect() -> None:
    global _redis, _redis_available
    host: str = os.getenv("REDIS_HOST", "127.0.0.1")
    port: int = int(os.getenv("REDIS_PORT", "6379"))
    password: str = os.getenv("REDIS_PASSWORD", "")

    try:
        _redis = redis_lib.Redis(
            host=host,
            port=port,
            password=password or None,
            decode_responses=True,
            socket_connect_timeout=2,
        )
        _redis.ping()
        _redis_available = True
        log("INFO", f"Redis connected at {host}:{port}")
    except Exception as exc:
        _redis_available = False
        log("WARN", f"Redis unavailable at {host}:{port} — {exc}")


def is_available() -> bool:
    _ensure_connected()
    return _redis_available


def _ensure_connected() -> None:
    global _connected
    if not _connected:
        connect()
        _connected = True


def _memory_incr(key: str, ttl: int) -> int:
    now = time.time()
    bucket = _mem_limiter[key]
    _mem_limiter[key] = [t for t in bucket if now - t < ttl]
    _mem_limiter[key].append(now)
    return len(_mem_limiter[key])


def get(key: str) -> str | None:
    _ensure_connected()
    if not _redis_available or _redis is None:
        return None
    try:
        return _redis.get(key)
    except Exception as exc:
        log("WARN", f"Redis get failed for {key}: {exc}")
        return None


def setex(key: str, value: str) -> None:
    _ensure_connected()
    if not _redis_available or _redis is None:
        return
    try:
        ttl_hours: int = get_settings().get("cache_ttl_hours", 24)
        if ttl_hours < 1:
            ttl_hours = 24
        _redis.setex(key, ttl_hours * 3600, value)
    except Exception as exc:
        log("WARN", f"Redis setex failed for {key}: {exc}")


def delete(key: str) -> None:
    _ensure_connected()
    if not _redis_available or _redis is None:
        return
    try:
        _redis.delete(key)
    except Exception as exc:
        log("WARN", f"Redis delete failed for {key}: {exc}")


def scan(count: int = 50) -> list[dict]:
    _ensure_connected()
    if not _redis_available or _redis is None:
        return []
    try:
        entries: list[tuple[str, str]] = []
        for pattern in ("fused:domain:*",):
            for key in _redis.scan_iter(match=pattern, count=count * 4):
                val = _redis.get(key)
                if val is None:
                    continue
                entries.append((key, val))

        entries.sort(key=lambda x: x[0], reverse=True)
        results: list[dict] = []
        for redis_key, val in entries[:count]:
            entry = json.loads(val)
            entry["cache_key"] = redis_key
            entry["is_fused"] = True
            enrich_screenshot_url(entry)
            results.append(entry)
        return results
    except Exception as exc:
        log("WARN", f"Redis scan failed: {exc}")
        return []


def flush_cache() -> int:
    _ensure_connected()
    if not _redis_available or _redis is None:
        return 0
    try:
        deleted: int = 0
        for pattern in ("fused:domain:*",):
            for key in _redis.scan_iter(match=pattern):
                _redis.delete(key)
                deleted += 1
        return deleted
    except Exception as exc:
        log("WARN", f"Redis flush failed: {exc}")
        return 0


def incr(key: str, ttl: int = 3600) -> int:
    _ensure_connected()
    if _redis_available and _redis is not None:
        try:
            count: int = _redis.incr(key)
            if count == 1:
                _redis.expire(key, ttl)
            return count
        except Exception as exc:
            log("WARN", f"Redis incr failed for {key}, fallback to in-memory: {exc}")
    return _memory_incr(key, ttl)
