from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from utils.lists import add_entry
from utils.reports import save_report


@patch("routers.report.cache_get", return_value="{}")
@patch("routers.report.cache_incr", return_value=1)
@patch("routers.report.cache_available", return_value=True)
def test_submit_report(
    _1: MagicMock, _2: MagicMock, _3: MagicMock, client: TestClient
) -> None:
    resp = client.post("/report", json={"url": "https://example.com/game", "gambling_score": 0.95})
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_submit_report_blacklisted_url(client: TestClient) -> None:
    add_entry("blocked.com", "blacklist")
    resp = client.post("/report", json={"url": "https://blocked.com/game", "gambling_score": 0.9})
    assert resp.status_code == 400
    assert "already_blacklisted" in resp.text


def test_submit_report_whitelisted_url(client: TestClient) -> None:
    add_entry("allowed.com", "whitelist")
    resp = client.post("/report", json={"url": "https://allowed.com/game", "gambling_score": 0.9})
    assert resp.status_code == 400
    assert "already_whitelisted" in resp.text
