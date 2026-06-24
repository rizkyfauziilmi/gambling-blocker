from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from utils.lists import add_entry
from utils.reports import save_report


def test_get_list_empty(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.get("/lists/blacklist", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["entries"] == []


def test_get_list_invalid_type(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.get("/lists/invalid", headers=auth_headers)
    assert resp.status_code == 400


def test_get_list_with_data(client: TestClient, auth_headers: dict[str, str]) -> None:
    add_entry("example.com", "blacklist")
    resp = client.get("/lists/blacklist", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert len(data["entries"]) == 1
    assert data["entries"][0]["hostname"] == "example.com"


def test_add_to_list(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post("/lists/blacklist", headers=auth_headers, json={"hostname": "https://bad.com"})
    assert resp.status_code == 200
    assert resp.json()["entry"]["hostname"] == "bad.com"


def test_add_duplicate_to_list(client: TestClient, auth_headers: dict[str, str]) -> None:
    client.post("/lists/blacklist", headers=auth_headers, json={"hostname": "bad.com"})
    resp = client.post("/lists/blacklist", headers=auth_headers, json={"hostname": "bad.com"})
    assert resp.status_code == 409


def test_delete_from_list(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.post("/lists/blacklist", headers=auth_headers, json={"hostname": "bad.com"})
    entry_id = resp.json()["entry"]["id"]
    resp = client.delete(f"/lists/blacklist/{entry_id}", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


def test_delete_nonexistent(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.delete("/lists/blacklist/999", headers=auth_headers)
    assert resp.status_code == 404


def test_get_cache_empty(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.get("/cache", headers=auth_headers)
    assert resp.status_code == 200
    assert resp.json()["entries"] == []


def test_get_logs(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.get("/logs", headers=auth_headers)
    assert resp.status_code == 200
    assert "entries" in resp.json()


def test_get_settings(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.get("/settings", headers=auth_headers)
    assert resp.status_code == 200


def test_update_settings(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.put("/settings", headers=auth_headers, json={"debug_logging_enabled": True})
    assert resp.status_code == 200
    assert resp.json()["debug_logging_enabled"] is True


def test_update_settings_invalid_field(client: TestClient, auth_headers: dict[str, str]) -> None:
    resp = client.put("/settings", headers=auth_headers, json={"unknown_field": True})
    assert resp.status_code == 400


def test_unauthorized_access(client: TestClient) -> None:
    resp = client.get("/lists/blacklist")
    assert resp.status_code == 401


def test_get_reports(client: TestClient, auth_headers: dict[str, str]) -> None:
    save_report("https://example.com", 0.5, "1.2.3.4")
    resp = client.get("/reports", headers=auth_headers)
    assert resp.status_code == 200
    data = resp.json()
    assert "groups" in data
    assert "stats" in data


def test_delete_report(client: TestClient, auth_headers: dict[str, str]) -> None:
    from db.models import Report
    from db import SessionLocal

    save_report("https://example.com", 0.5, "1.2.3.4")
    with SessionLocal() as s:
        report = s.query(Report).first()
        rid = report.id
    resp = client.delete(f"/reports/{rid}", headers=auth_headers)
    assert resp.status_code == 200
