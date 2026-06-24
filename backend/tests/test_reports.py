from __future__ import annotations

from datetime import datetime, timezone

import pytest
from sqlalchemy.orm import Session

from db.models import Report
from utils.reports import delete_report, delete_reports_by_hostname, get_grouped_reports, get_report_stats, save_report


def test_save_report(test_db: Session) -> None:
    save_report("https://example.com/game", 0.95, "1.2.3.4")
    stats = get_report_stats()
    assert stats["total"] == 1
    assert stats["unique_hostnames"] == 1


def test_get_report_stats_today(test_db: Session) -> None:
    save_report("https://example.com", 0.5, "1.2.3.4")
    stats = get_report_stats()
    assert stats["today"] == 1


def test_get_report_stats_multiple_hostnames(test_db: Session) -> None:
    save_report("https://a.com", 0.9, "1.2.3.4")
    save_report("https://b.com", 0.8, "1.2.3.4")
    save_report("https://a.com/page2", 0.7, "1.2.3.4")
    stats = get_report_stats()
    assert stats["total"] == 3
    assert stats["unique_hostnames"] == 2


def test_delete_report(test_db: Session) -> None:
    save_report("https://example.com", 0.5, "1.2.3.4")
    test_db.expire_all()
    report = test_db.query(Report).first()
    assert report is not None
    assert delete_report(report.id) is True
    assert get_report_stats()["total"] == 0


def test_delete_report_not_found(test_db: Session) -> None:
    assert delete_report(999) is False


def test_delete_reports_by_hostname(test_db: Session) -> None:
    save_report("https://a.com/1", 0.5, "1.2.3.4")
    save_report("https://a.com/2", 0.6, "1.2.3.4")
    save_report("https://b.com", 0.7, "1.2.3.4")
    delete_reports_by_hostname("a.com")
    stats = get_report_stats()
    assert stats["total"] == 1


def test_get_grouped_reports(test_db: Session) -> None:
    save_report("https://a.com/game", 0.9, "1.2.3.4")
    save_report("https://a.com/casino", 0.8, "1.2.3.4")
    save_report("https://b.com/bet", 0.7, "1.2.3.4")
    groups = get_grouped_reports()
    assert len(groups) == 2
    a_group = next(g for g in groups if g["hostname"] == "a.com")
    assert a_group["report_count"] == 2
