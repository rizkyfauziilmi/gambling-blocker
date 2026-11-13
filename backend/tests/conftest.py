from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

os.environ["DASHBOARD_USERNAME"] = "admin"
os.environ["DASHBOARD_PASSWORD"] = "admin123"

import base64
from collections.abc import Generator

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import StaticPool

from db.models import Base

TestEngine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=False,
)
TestSessionLocal = sessionmaker(bind=TestEngine)


@pytest.fixture(autouse=True)
def test_db(monkeypatch: pytest.MonkeyPatch) -> Generator[Session, None, None]:
    Base.metadata.create_all(bind=TestEngine)
    monkeypatch.setattr("db.engine", TestEngine)
    monkeypatch.setattr("db.models.engine", TestEngine)
    monkeypatch.setattr("db.SessionLocal", TestSessionLocal)
    monkeypatch.setattr("utils.lists.SessionLocal", TestSessionLocal)
    monkeypatch.setattr("utils.reports.SessionLocal", TestSessionLocal)
    monkeypatch.setattr("utils.extensions.SessionLocal", TestSessionLocal)
    with TestSessionLocal() as session:
        yield session
    Base.metadata.drop_all(bind=TestEngine)


@pytest.fixture(autouse=True)
def mock_model(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("utils.model._model_loaded", True)
    monkeypatch.setattr("utils.model.load", lambda: True)
    monkeypatch.setattr("utils.model.is_loaded", lambda: True)


@pytest.fixture(autouse=True)
def mock_cache(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("utils.cache._connected", True)
    monkeypatch.setattr("utils.cache.connect", lambda: None)


@pytest.fixture(autouse=True)
def mock_smtp(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("utils.email._smtp_connect", lambda: None)


@pytest.fixture(autouse=True)
def mock_storage(monkeypatch: pytest.MonkeyPatch) -> None:
    from unittest.mock import MagicMock

    storage_mock = MagicMock()
    storage_mock.is_ready.return_value = False
    storage_mock.upload_bytes.return_value = False
    storage_mock.presigned_url.return_value = None
    monkeypatch.setattr("utils.storage.get_storage", lambda: storage_mock)


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    from main import app

    with TestClient(app) as c:
        yield c


@pytest.fixture
def auth_headers() -> dict[str, str]:
    creds = base64.b64encode(b"admin:admin123").decode()
    return {"Authorization": f"Basic {creds}"}
