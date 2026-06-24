from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from db.models import init_db
from routers.classify import router as classify_router
from routers.dashboard import router as dashboard_router
from routers.extension import router as extension_router
from routers.report import router as report_router
from utils.cache import connect as cache_connect
from utils.email import send_heartbeat_stale_alert
from utils.extensions import get_stale_extensions, mark_stale_alerted
from utils.logger import log as log_msg
from utils.model import load as model_load
from utils.settings import get as settings_get

_scheduler: Any | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    cache_connect()
    model_load()
    global _scheduler
    try:
        from apscheduler.schedulers.background import BackgroundScheduler

        interval = settings_get().get("stale_check_interval_minutes", 30)
        _scheduler = BackgroundScheduler()
        _scheduler.add_job(
            _check_stale_heartbeats,
            "interval",
            minutes=interval,
            id="heartbeat_monitor",
        )
        _scheduler.start()
        _app.state.scheduler = _scheduler
        log_msg("API", f"APScheduler started: heartbeat monitor every {interval}m")
    except Exception as exc:
        _app.state.scheduler = None
        log_msg("WARN", f"APScheduler not available: {exc}")
    yield


app: FastAPI = FastAPI(lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

app.include_router(classify_router)
app.include_router(extension_router)
app.include_router(report_router)
app.include_router(dashboard_router)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "url gambling classifier", "status": "running"}


def _check_stale_heartbeats() -> None:
    hours = settings_get().get("stale_hours", 2)
    stale = get_stale_extensions(hours)
    if not stale:
        log_msg("HEARTBEAT", f"stale check: 0 stale (threshold={hours}h)")
        return

    log_msg("HEARTBEAT", f"stale check: {len(stale)} extension(s) stale (> {hours}h)")

    from utils.email import _smtp_connect

    try:
        server = _smtp_connect()
    except Exception as exc:
        log_msg("EMAIL", f"SMTP connect failed for batch stale alert: {exc}")
        return

    if server is None:
        log_msg("EMAIL", "SMTP not configured — skipping stale alerts")
        return

    try:
        for ext in stale:
            partner_email = ext.get("partner_email", "")
            if partner_email:
                send_heartbeat_stale_alert(
                    partner_email, hours_since_last=hours, server=server
                )
                mark_stale_alerted(ext["extension_id"])
                log_msg(
                    "HEARTBEAT",
                    f"stale alert sent for {ext['extension_id']}"
                    f" \u2192 {partner_email}",
                )
    finally:
        try:
            server.quit()
        except Exception:
            pass
