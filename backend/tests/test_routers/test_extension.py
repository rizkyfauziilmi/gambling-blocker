from __future__ import annotations

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from utils.extensions import setup_partner


def test_heartbeat(client: TestClient) -> None:
    resp = client.post("/extension/heartbeat", json={"extension_id": "ext-1"})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_tamper_alert(client: TestClient) -> None:
    resp = client.post("/extension/tamper-alert", json={"extension_id": "ext-1", "event_type": "failed_password"})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_gambling_alert(client: TestClient) -> None:
    resp = client.post("/extension/gambling-alert", json={"extension_id": "ext-1", "url": "https://casino.com", "gambling_score": 0.99})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True


def test_setup_partner_email_sent(client: TestClient) -> None:
    with patch("routers.extension.send_partner_password", return_value=True):
        resp = client.post("/extension/setup", json={"extension_id": "ext-1", "partner_email": "partner@example.com"})
    assert resp.status_code == 200
    assert resp.json()["success"] is True
    assert "password_hash" in resp.json()


def test_setup_partner_email_failed(client: TestClient) -> None:
    with patch("routers.extension.send_partner_password", return_value=False):
        resp = client.post("/extension/setup", json={"extension_id": "ext-1", "partner_email": "partner@example.com"})
    assert resp.status_code == 502


def test_reset_password(client: TestClient) -> None:
    with patch("routers.extension.send_partner_password", return_value=True):
        client.post("/extension/setup", json={"extension_id": "ext-1", "partner_email": "p@example.com"})
    with patch("routers.extension.send_reset_password", return_value=True):
        resp = client.post("/extension/reset-password", json={"extension_id": "ext-1"})
    assert resp.status_code == 200
    assert resp.json()["success"] is True


def test_reset_password_not_found(client: TestClient) -> None:
    resp = client.post("/extension/reset-password", json={"extension_id": "ext-unknown"})
    assert resp.status_code == 404


def test_status_requires_auth(client: TestClient) -> None:
    resp = client.get("/extension/status", params={"extension_id": "ext-1"})
    assert resp.status_code == 401


def test_heartbeats_requires_auth(client: TestClient) -> None:
    resp = client.get("/extension/heartbeats")
    assert resp.status_code == 401
