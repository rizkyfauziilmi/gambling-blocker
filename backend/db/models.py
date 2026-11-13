from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import Float, ForeignKey, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from db import engine


class Base(DeclarativeBase):
    pass


class PartnerAccount(Base):
    __tablename__ = "partner_accounts"

    extension_id: Mapped[str] = mapped_column(String, primary_key=True)
    partner_email: Mapped[str] = mapped_column(String, nullable=False)
    password_hash: Mapped[str] = mapped_column(String, nullable=False)
    password_salt: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(
        String, nullable=False, default=lambda: datetime.now(timezone.utc).isoformat()
    )
    updated_at: Mapped[str] = mapped_column(
        String, nullable=False, default=lambda: datetime.now(timezone.utc).isoformat()
    )
    stale_alerted_at: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    heartbeats: Mapped[list[Heartbeat]] = relationship(
        back_populates="partner", cascade="all, delete-orphan"
    )
    tamper_logs: Mapped[list[TamperLog]] = relationship(
        back_populates="partner", cascade="all, delete-orphan"
    )


class Heartbeat(Base):
    __tablename__ = "heartbeats"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    extension_id: Mapped[str] = mapped_column(
        String, ForeignKey("partner_accounts.extension_id"), nullable=False
    )
    timestamp: Mapped[str] = mapped_column(String, nullable=False)
    ip_address: Mapped[Optional[str]] = mapped_column(String, nullable=True)

    partner: Mapped[PartnerAccount] = relationship(back_populates="heartbeats")


class TamperLog(Base):
    __tablename__ = "tamper_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    extension_id: Mapped[str] = mapped_column(
        String, ForeignKey("partner_accounts.extension_id"), nullable=False
    )
    event_type: Mapped[str] = mapped_column(String, nullable=False)
    details: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    timestamp: Mapped[str] = mapped_column(
        String, nullable=False, default=lambda: datetime.now(timezone.utc).isoformat()
    )

    partner: Mapped[PartnerAccount] = relationship(back_populates="tamper_logs")


class SiteList(Base):
    __tablename__ = "site_lists"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    hostname: Mapped[str] = mapped_column(String, unique=True, nullable=False)
    list_type: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(
        String, nullable=False, default=lambda: datetime.now(timezone.utc).isoformat()
    )


class Report(Base):
    __tablename__ = "reports"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    url: Mapped[str] = mapped_column(String, nullable=False)
    hostname: Mapped[str] = mapped_column(String, nullable=False)
    gambling_score: Mapped[float] = mapped_column(Float, nullable=False)
    reporter_ip: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[str] = mapped_column(
        String, nullable=False, default=lambda: datetime.now(timezone.utc).isoformat()
    )


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
