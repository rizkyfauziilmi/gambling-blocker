from __future__ import annotations

import time

import pytest

from utils.cache import incr, is_available


def test_cache_not_available_without_redis() -> None:
    assert is_available() is False


@pytest.fixture(autouse=False)
def reset_mem_limiter() -> None:
    from utils.cache import _mem_limiter

    _mem_limiter.clear()


def test_incr_in_memory_first_call() -> None:
    count = incr("rate:test:key", ttl=60)
    assert count == 1


def test_incr_in_memory_multiple_calls() -> None:
    key = "rate:test:multi"
    for i in range(1, 6):
        count = incr(key, ttl=60)
        assert count == i


def test_incr_in_memory_rate_limit_threshold() -> None:
    key = "rate:test:threshold"
    for _ in range(10):
        incr(key, ttl=60)
    count = incr(key, ttl=60)
    assert count == 11  # > 10 → rate limited


def test_incr_in_memory_ttl_expiry(reset_mem_limiter: None) -> None:
    key = "rate:test:ttl"
    incr(key, ttl=1)
    assert incr(key, ttl=1) == 2
    time.sleep(1.1)
    count = incr(key, ttl=1)
    assert count == 1  # expired


def test_incr_different_keys_independent() -> None:
    c1 = incr("rate:key:a", ttl=60)
    c2 = incr("rate:key:b", ttl=60)
    assert c1 == 1
    assert c2 == 1
