from __future__ import annotations

from datetime import datetime, timezone
from urllib.parse import urlparse

from sqlalchemy import func, select

from db import SessionLocal
from db.models import Report


def save_report(url: str, gambling_score: float, reporter_ip: str) -> None:
    hostname: str = urlparse(url).hostname or url
    with SessionLocal() as session:
        report = Report(
            url=url,
            hostname=hostname,
            gambling_score=gambling_score,
            reporter_ip=reporter_ip,
        )
        session.add(report)
        session.commit()


def delete_reports_by_hostname(hostname: str) -> None:
    with SessionLocal() as session:
        session.query(Report).where(Report.hostname == hostname).delete()
        session.commit()


def delete_report(report_id: int) -> bool:
    with SessionLocal() as session:
        report = session.execute(
            select(Report).where(Report.id == report_id)
        ).scalar_one_or_none()
        if report is None:
            return False
        session.delete(report)
        session.commit()
        return True


def get_report_stats() -> dict[str, int]:
    today_start = (
        datetime.now(timezone.utc)
        .replace(hour=0, minute=0, second=0, microsecond=0)
        .isoformat()
    )
    with SessionLocal() as session:
        total = session.execute(select(func.count(Report.id))).scalar() or 0
        today = (
            session.execute(
                select(func.count(Report.id)).where(Report.created_at >= today_start)
            ).scalar()
            or 0
        )
        unique_hostnames = (
            session.execute(select(func.count(func.distinct(Report.hostname)))).scalar()
            or 0
        )
    return {"total": total, "today": today, "unique_hostnames": unique_hostnames}


def get_grouped_reports() -> list[dict]:
    with SessionLocal() as session:
        rows = session.execute(
            select(
                Report.hostname,
                func.count(Report.id).label("report_count"),
                func.round(func.avg(Report.gambling_score), 4).label("avg_score"),
                func.max(Report.created_at).label("last_reported"),
            )
            .group_by(Report.hostname)
            .order_by(func.count(Report.id).desc(), func.max(Report.created_at).desc())
        ).all()
    return [
        {
            "hostname": r.hostname,
            "report_count": r.report_count,
            "avg_score": r.avg_score,
            "last_reported": r.last_reported,
        }
        for r in rows
    ]
