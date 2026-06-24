from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timezone

from sqlalchemy import func, select, update

from db import SessionLocal
from db.models import Heartbeat, PartnerAccount, TamperLog


def _hash_password(password: str, salt: str | None = None) -> tuple[str, str]:
    if salt is None:
        salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 600000)
    return h.hex(), salt


def setup_partner(extension_id: str, partner_email: str) -> dict:
    password = secrets.token_urlsafe(12)
    pw_hash, salt = _hash_password(password)
    now = datetime.now(timezone.utc).isoformat()
    with SessionLocal() as session:
        existing = session.execute(
            select(PartnerAccount).where(PartnerAccount.extension_id == extension_id)
        ).scalar_one_or_none()

        if existing:
            existing.partner_email = partner_email
            existing.password_hash = pw_hash
            existing.password_salt = salt
            existing.updated_at = now
        else:
            partner = PartnerAccount(
                extension_id=extension_id,
                partner_email=partner_email,
                password_hash=pw_hash,
                password_salt=salt,
                created_at=now,
                updated_at=now,
            )
            session.add(partner)
        session.commit()
    return {"password": password, "password_hash": pw_hash, "password_salt": salt}


def restore_partner(extension_id: str, password_hash: str, password_salt: str) -> None:
    with SessionLocal() as session:
        session.execute(
            update(PartnerAccount)
            .where(PartnerAccount.extension_id == extension_id)
            .values(
                password_hash=password_hash,
                password_salt=password_salt,
                updated_at=datetime.now(timezone.utc).isoformat(),
            )
        )
        session.commit()


def delete_partner(extension_id: str) -> None:
    with SessionLocal() as session:
        partner = session.execute(
            select(PartnerAccount).where(PartnerAccount.extension_id == extension_id)
        ).scalar_one_or_none()
        if partner is None:
            return
        session.delete(partner)
        session.commit()


def record_heartbeat(extension_id: str, ip_address: str | None = None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with SessionLocal() as session:
        heartbeat = Heartbeat(
            extension_id=extension_id, timestamp=now, ip_address=ip_address
        )
        session.add(heartbeat)
        session.execute(
            update(PartnerAccount)
            .where(PartnerAccount.extension_id == extension_id)
            .values(stale_alerted_at=None)
        )
        session.commit()


def log_tamper(extension_id: str, event_type: str, details: str = "") -> None:
    now = datetime.now(timezone.utc).isoformat()
    with SessionLocal() as session:
        tamper = TamperLog(
            extension_id=extension_id,
            event_type=event_type,
            details=details,
            timestamp=now,
        )
        session.add(tamper)
        session.commit()


def mark_stale_alerted(extension_id: str) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with SessionLocal() as session:
        session.execute(
            update(PartnerAccount)
            .where(PartnerAccount.extension_id == extension_id)
            .values(stale_alerted_at=now)
        )
        session.commit()


def get_partner(extension_id: str) -> dict | None:
    with SessionLocal() as session:
        row = session.execute(
            select(PartnerAccount).where(PartnerAccount.extension_id == extension_id)
        ).scalar_one_or_none()
    if row is None:
        return None
    return {
        "extension_id": row.extension_id,
        "partner_email": row.partner_email,
        "password_hash": row.password_hash,
        "password_salt": row.password_salt,
        "created_at": row.created_at,
        "updated_at": row.updated_at,
        "stale_alerted_at": row.stale_alerted_at,
    }


def get_tamper_count(extension_id: str, hours: int = 1) -> int:
    with SessionLocal() as session:
        row = session.execute(
            select(func.count(TamperLog.id)).where(
                TamperLog.extension_id == extension_id,
                TamperLog.event_type == "extensions_page",
                func.datetime(TamperLog.timestamp)
                > func.datetime("now", f"-{hours} hours"),
            )
        ).scalar()
    return row or 0


def get_stale_extensions(hours: int = 24) -> list[dict]:
    with SessionLocal() as session:
        rows = session.execute(
            select(
                PartnerAccount,
                func.max(Heartbeat.timestamp).label("last_heartbeat"),
            )
            .outerjoin(
                Heartbeat,
                Heartbeat.extension_id == PartnerAccount.extension_id,
            )
            .group_by(PartnerAccount.extension_id)
            .having(
                func.max(Heartbeat.timestamp).is_(None)
                | (
                    func.datetime(func.max(Heartbeat.timestamp))
                    < func.datetime("now", f"-{hours} hours")
                )
            )
            .where(PartnerAccount.stale_alerted_at.is_(None))
        ).all()
    result = []
    for r in rows:
        partner: PartnerAccount = r[0]
        d = {
            "extension_id": partner.extension_id,
            "partner_email": partner.partner_email,
            "password_hash": partner.password_hash,
            "password_salt": partner.password_salt,
            "created_at": partner.created_at,
            "updated_at": partner.updated_at,
            "stale_alerted_at": partner.stale_alerted_at,
            "last_heartbeat": r.last_heartbeat,
        }
        result.append(d)
    return result


def get_status(extension_id: str) -> dict:
    partner = get_partner(extension_id)
    if partner is None:
        return {"exists": False}

    with SessionLocal() as session:
        last_heartbeat_at = session.execute(
            select(func.max(Heartbeat.timestamp)).where(
                Heartbeat.extension_id == extension_id
            )
        ).scalar()

    age: int | None = None
    if last_heartbeat_at:
        last = datetime.fromisoformat(last_heartbeat_at)
        age = int((datetime.now(timezone.utc) - last).total_seconds() // 3600)

    tamper_count = get_tamper_count(extension_id, 1)
    return {
        "exists": True,
        "partner_email": partner["partner_email"],
        "last_heartbeat_at": last_heartbeat_at,
        "heartbeat_age_hours": age,
        "tamper_count_1h": tamper_count,
    }


def get_all_heartbeat_status() -> list[dict]:
    with SessionLocal() as session:
        rows = session.execute(
            select(
                PartnerAccount.extension_id,
                PartnerAccount.partner_email,
                PartnerAccount.stale_alerted_at,
                func.max(Heartbeat.timestamp).label("last_heartbeat_at"),
                func.count(Heartbeat.id).label("total_heartbeats"),
            )
            .outerjoin(
                Heartbeat,
                Heartbeat.extension_id == PartnerAccount.extension_id,
            )
            .group_by(PartnerAccount.extension_id)
            .order_by(func.max(Heartbeat.timestamp).desc().nullslast())
        ).all()

    now = datetime.now(timezone.utc)
    result = []
    for r in rows:
        d = {
            "extension_id": r.extension_id,
            "partner_email": r.partner_email,
            "stale_alerted_at": r.stale_alerted_at,
            "last_heartbeat_at": r.last_heartbeat_at,
            "total_heartbeats": r.total_heartbeats,
        }
        if d["last_heartbeat_at"]:
            last = datetime.fromisoformat(d["last_heartbeat_at"])
            d["heartbeat_age_hours"] = int((now - last).total_seconds() // 3600)
        else:
            d["heartbeat_age_hours"] = None
        result.append(d)
    return result


def delete_heartbeats(extension_id: str) -> None:
    with SessionLocal() as session:
        session.query(Heartbeat).where(Heartbeat.extension_id == extension_id).delete()
        session.commit()
