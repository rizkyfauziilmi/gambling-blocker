from __future__ import annotations

import pytest
from sqlalchemy.orm import Session

from utils.lists import add_entry, check_hostname, get_entries, remove_entry


def test_add_and_get_blacklist(test_db: Session) -> None:
    entry = add_entry("example.com", "blacklist")
    assert entry is not None
    assert entry["hostname"] == "example.com"
    assert entry["list_type"] == "blacklist"
    assert "id" in entry
    assert "created_at" in entry

    entries = get_entries("blacklist")
    assert len(entries) == 1
    assert entries[0]["hostname"] == "example.com"


def test_add_and_get_whitelist(test_db: Session) -> None:
    entry = add_entry("allowed.com", "whitelist")
    assert entry is not None
    assert entry["list_type"] == "whitelist"

    entries = get_entries("whitelist")
    assert len(entries) == 1
    assert entries[0]["hostname"] == "allowed.com"


def test_add_duplicate_hostname(test_db: Session) -> None:
    add_entry("example.com", "blacklist")
    result = add_entry("example.com", "whitelist")
    assert result is None


def test_get_empty_list(test_db: Session) -> None:
    entries = get_entries("blacklist")
    assert entries == []


def test_remove_entry(test_db: Session) -> None:
    entry = add_entry("example.com", "blacklist")
    assert remove_entry(entry["id"]) is True
    assert get_entries("blacklist") == []


def test_remove_nonexistent(test_db: Session) -> None:
    assert remove_entry(999) is False


def test_check_hostname_found(test_db: Session) -> None:
    add_entry("example.com", "blacklist")
    assert check_hostname("example.com") == "blacklist"


def test_check_hostname_not_found(test_db: Session) -> None:
    assert check_hostname("unknown.com") is None


def test_two_lists_isolated(test_db: Session) -> None:
    add_entry("a.com", "blacklist")
    add_entry("b.com", "whitelist")
    assert len(get_entries("blacklist")) == 1
    assert len(get_entries("whitelist")) == 1
