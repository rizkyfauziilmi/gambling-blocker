from __future__ import annotations

from sqlalchemy import select

from db import SessionLocal
from db.models import SiteList


def add_entry(hostname: str, list_type: str) -> dict[str, object] | None:
    with SessionLocal() as session:
        existing = session.execute(
            select(SiteList).where(SiteList.hostname == hostname)
        ).scalar_one_or_none()
        if existing is not None:
            return None
        entry = SiteList(hostname=hostname, list_type=list_type)
        session.add(entry)
        session.commit()
        session.refresh(entry)
        return {
            "id": entry.id,
            "hostname": entry.hostname,
            "list_type": entry.list_type,
            "created_at": entry.created_at,
        }


def remove_entry(entry_id: int) -> bool:
    with SessionLocal() as session:
        entry = session.execute(
            select(SiteList).where(SiteList.id == entry_id)
        ).scalar_one_or_none()
        if entry is None:
            return False
        session.delete(entry)
        session.commit()
        return True


def get_entries(list_type: str) -> list[dict[str, object]]:
    with SessionLocal() as session:
        rows = (
            session.execute(
                select(SiteList)
                .where(SiteList.list_type == list_type)
                .order_by(SiteList.created_at.desc())
            )
            .scalars()
            .all()
        )
        return [
            {
                "id": r.id,
                "hostname": r.hostname,
                "list_type": r.list_type,
                "created_at": r.created_at,
            }
            for r in rows
        ]


def check_hostname(hostname: str) -> str | None:
    with SessionLocal() as session:
        result = session.execute(
            select(SiteList.list_type).where(SiteList.hostname == hostname)
        ).scalar_one_or_none()
        return result
