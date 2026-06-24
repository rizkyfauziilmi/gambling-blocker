from __future__ import annotations

from datetime import datetime, timezone, timedelta

import pytest
from sqlalchemy.orm import Session

from db.models import Heartbeat, PartnerAccount, TamperLog
from utils.extensions import get_stale_extensions, get_tamper_count, log_tamper, record_heartbeat, setup_partner


def test_get_tamper_count_within_window(test_db: Session) -> None:
    log_tamper("ext-1", "extensions_page", "test")
    count = get_tamper_count("ext-1", hours=1)
    assert count == 1


def test_get_tamper_count_outside_window(test_db: Session) -> None:
    t = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    test_db.add(TamperLog(extension_id="ext-1", event_type="extensions_page", details="old", timestamp=t))
    test_db.commit()
    count = get_tamper_count("ext-1", hours=1)
    assert count == 0


def test_get_tamper_count_wrong_event_type(test_db: Session) -> None:
    log_tamper("ext-1", "failed_password", "wrong pw")
    count = get_tamper_count("ext-1", hours=1)
    assert count == 0


def test_get_tamper_count_no_tamper(test_db: Session) -> None:
    count = get_tamper_count("ext-1", hours=1)
    assert count == 0


def test_setup_partner_creates_entry(test_db: Session) -> None:
    result = setup_partner("ext-1", "partner@example.com")
    assert result["password"] is not None
    row = test_db.query(PartnerAccount).filter_by(extension_id="ext-1").first()
    assert row is not None
    assert row.partner_email == "partner@example.com"


def test_setup_partner_updates_existing(test_db: Session) -> None:
    setup_partner("ext-1", "old@example.com")
    setup_partner("ext-1", "new@example.com")
    rows = test_db.query(PartnerAccount).filter_by(extension_id="ext-1").all()
    assert len(rows) == 1
    assert rows[0].partner_email == "new@example.com"


def test_record_heartbeat(test_db: Session) -> None:
    record_heartbeat("ext-1")
    hb = test_db.query(Heartbeat).filter_by(extension_id="ext-1").first()
    assert hb is not None
    assert hb.extension_id == "ext-1"


def test_get_stale_extensions_no_partners(test_db: Session) -> None:
    stale = get_stale_extensions(hours=24)
    assert stale == []


def test_get_stale_extensions_with_recent_heartbeat(test_db: Session) -> None:
    setup_partner("ext-1", "p@example.com")
    record_heartbeat("ext-1")
    stale = get_stale_extensions(hours=24)
    assert stale == []


def test_get_stale_extensions_without_heartbeat(test_db: Session) -> None:
    setup_partner("ext-1", "p@example.com")
    stale = get_stale_extensions(hours=24)
    assert len(stale) == 1
    assert stale[0]["extension_id"] == "ext-1"
