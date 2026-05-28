import json
import os

import redis as redis_lib

from .config import CACHE_TTL

_redis: redis_lib.Redis | None = None
_redis_available: bool = False


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
        print(f"[INFO] Redis connected at {host}:{port}")
    except Exception as exc:
        _redis_available = False
        print(f"[WARN] Redis unavailable at {host}:{port} — {exc}")


def is_available() -> bool:
    return _redis_available


def get(key: str) -> str | None:
    if not _redis_available or _redis is None:
        return None
    try:
        return _redis.get(key)  # type: ignore[assignment]
    except Exception:
        return None


def setex(key: str, value: str) -> None:
    if not _redis_available or _redis is None:
        return
    try:
        _redis.setex(key, CACHE_TTL, value)
    except Exception:
        pass


def delete(key: str) -> None:
    if not _redis_available or _redis is None:
        return
    try:
        _redis.delete(key)
    except Exception:
        pass


def scan(count: int = 50) -> list[dict]:
    if not _redis_available or _redis is None:
        return []
    try:
        keys: list[str] = []
        for pattern in ("domain:*", "ip:*"):
            for key in _redis.scan_iter(match=pattern, count=count * 2):
                keys.append(key)
        results: list[dict] = []
        for key in sorted(keys, reverse=True)[:count]:
            val = _redis.get(key)
            if val:
                entry = json.loads(val)
                entry["cache_key"] = key
                results.append(entry)
        return results
    except Exception:
        return []


def flush_cache() -> int:
    if not _redis_available or _redis is None:
        return 0
    try:
        deleted: int = 0
        for pattern in ("domain:*", "ip:*"):
            for key in _redis.scan_iter(match=pattern):
                _redis.delete(key)
                deleted += 1
        return deleted
    except Exception:
        return 0


def incr(key: str, ttl: int = 3600) -> int:
    if not _redis_available or _redis is None:
        return 0
    try:
        count: int = _redis.incr(key)
        if count == 1:
            _redis.expire(key, ttl)
        return count
    except Exception:
        return 0


connect()
