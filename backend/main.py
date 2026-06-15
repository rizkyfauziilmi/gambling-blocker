import json
import os
from contextlib import asynccontextmanager
from typing import Annotated, Any
from urllib.parse import urlparse

import dns.resolver
from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import AfterValidator, AnyHttpUrl, BaseModel, EmailStr

from utils.cache import delete as cache_delete
from utils.cache import flush_cache as cache_flush
from utils.cache import get as cache_get
from utils.cache import incr as cache_incr
from utils.cache import is_available as cache_available
from utils.cache import scan as cache_scan
from utils.cache import setex as cache_setex
from utils.email import (
    send_heartbeat_stale_alert,
    send_partner_password,
    send_reset_password,
    send_tamper_alert,
)
from utils.extensions import (
    delete_heartbeats as ext_delete_heartbeats,
)
from utils.extensions import (
    delete_partner,
    get_all_heartbeat_status,
    get_partner,
    log_tamper,
    record_heartbeat,
    restore_partner,
    setup_partner,
)
from utils.extensions import (
    get_status as ext_get_status,
)
from utils.helpers import cache_key, is_ip, parse_hostname
from utils.lists import add_entry as list_add
from utils.lists import check_hostname as list_check
from utils.lists import get_entries as list_get
from utils.lists import remove_entry as list_remove
from utils.logger import clear as logs_clear
from utils.logger import get_logs as logs_get
from utils.logger import log as log_msg
from utils.model import infer_fused
from utils.model import is_loaded as model_loaded
from utils.reports import delete_report as reports_delete
from utils.reports import delete_reports_by_hostname as reports_delete_by_host
from utils.reports import get_grouped_reports, get_report_stats, save_report
from utils.settings import get as settings_get
from utils.settings import save as settings_save
from utils.storage import enrich_screenshot_url

_scheduler: Any | None = None


@asynccontextmanager
async def lifespan(_app: FastAPI):
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
        log_msg("API", f"APScheduler started: heartbeat monitor every {interval}m")
    except Exception as exc:
        log_msg("WARN", f"APScheduler not available: {exc}")
    yield


app: FastAPI = FastAPI(lifespan=lifespan)


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

security = HTTPBasic()
DASHBOARD_USER: str = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASS: str = os.getenv("DASHBOARD_PASSWORD", "admin123")


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    if credentials.username != DASHBOARD_USER or credentials.password != DASHBOARD_PASS:
        raise HTTPException(status_code=401)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "url gambling classifier", "status": "running"}


def _check_stale_heartbeats() -> None:
    from utils.extensions import get_stale_extensions, mark_stale_alerted

    hours = settings_get().get("stale_hours", 2)
    stale = get_stale_extensions(hours)
    if stale:
        log_msg(
            "HEARTBEAT", f"stale check: {len(stale)} extension(s) stale (> {hours}h)"
        )
    for ext in stale:
        partner_email = ext.get("partner_email", "")
        if partner_email:
            send_heartbeat_stale_alert(partner_email, hours_since_last=hours)
            mark_stale_alerted(ext["extension_id"])
            log_msg(
                "HEARTBEAT",
                f"stale alert sent for {ext['extension_id']} → {partner_email}",
            )
    if not stale:
        log_msg("HEARTBEAT", f"stale check: 0 stale (threshold={hours}h)")


def check_email_mx(v: str) -> str:
    domain = v.split("@")[1]
    try:
        dns.resolver.resolve(domain, "MX", lifetime=5)
    except dns.resolver.LifetimeTimeout:
        raise ValueError(f"DNS timeout checking domain {domain}")
    except (dns.resolver.NoAnswer, dns.resolver.NXDOMAIN):
        raise ValueError(f"Domain {domain} does not accept email (no MX record)")
    return v


class ExtensionSetupBody(BaseModel):
    extension_id: str
    partner_email: Annotated[EmailStr, AfterValidator(check_email_mx)]


@app.post("/extension/setup")
def extension_setup(body: ExtensionSetupBody) -> dict:
    result = setup_partner(body.extension_id, body.partner_email)
    if settings_get().get("auto_heartbeat_on_setup", True):
        record_heartbeat(body.extension_id)
        log_msg(
            "HEARTBEAT",
            f"initial heartbeat recorded for {body.extension_id} (via partner setup)",
        )
    else:
        log_msg(
            "HEARTBEAT",
            f"initial heartbeat SKIPPED for {body.extension_id} (auto_heartbeat_on_setup=false)",  # noqa: E501
        )
    log_msg("PARTNER", f"partner set up for {body.extension_id} → {body.partner_email}")
    email_ok = send_partner_password(body.partner_email, result["password"])
    if not email_ok:
        log_msg("PARTNER", f"email FAILED to {body.partner_email}, rolling back")
        delete_partner(body.extension_id)
        raise HTTPException(
            status_code=502,
            detail={"error": "email_failed", "message": "Failed to send partner email"},
        )
    log_msg("PARTNER", f"email sent to {body.partner_email}")
    return {
        "success": True,
        "password_hash": result["password_hash"],
        "password_salt": result["password_salt"],
    }


class ExtensionHeartbeatBody(BaseModel):
    extension_id: str


@app.post("/extension/heartbeat")
def extension_heartbeat(body: ExtensionHeartbeatBody, request: Request) -> dict:
    ip = request.client.host if request.client else None
    record_heartbeat(body.extension_id, ip)
    log_msg("HEARTBEAT", f"from {body.extension_id}" + (f" ({ip})" if ip else ""))
    return {"ok": True}


class ExtensionTamperBody(BaseModel):
    extension_id: str
    event_type: str
    details: str = ""


@app.post("/extension/tamper-alert")
def extension_tamper_alert(body: ExtensionTamperBody) -> dict:
    log_tamper(body.extension_id, body.event_type, body.details)
    log_msg(
        "TAMPER",
        f"{body.event_type} from {body.extension_id}"
        + (f": {body.details}" if body.details else ""),
    )
    partner = get_partner(body.extension_id)
    if partner:
        send_tamper_alert(
            partner["partner_email"],
            body.event_type,
            details=body.details,
        )
    return {"ok": True}


class ExtensionResetBody(BaseModel):
    extension_id: str


@app.post("/extension/reset-password")
def extension_reset_password(body: ExtensionResetBody) -> dict:
    partner = get_partner(body.extension_id)
    if not partner:
        raise HTTPException(status_code=404, detail="Extension not registered")
    old_hash = partner["password_hash"]
    old_salt = partner["password_salt"]
    result = setup_partner(body.extension_id, partner["partner_email"])
    log_msg("PARTNER", f"password reset for {body.extension_id}")
    email_ok = send_reset_password(partner["partner_email"], result["password"])
    if not email_ok:
        log_msg(
            "PARTNER",
            f"email FAILED on reset to {partner['partner_email']}, rolling back",
        )
        restore_partner(body.extension_id, old_hash, old_salt)
        raise HTTPException(status_code=502, detail="Failed to send email")
    log_msg("PARTNER", f"new password emailed to {partner['partner_email']}")
    return {
        "success": True,
        "password_hash": result["password_hash"],
        "password_salt": result["password_salt"],
    }


@app.get("/extension/status")
def extension_status(
    extension_id: str = Query(...),
    _: None = Depends(require_auth),
) -> dict:
    return ext_get_status(extension_id)


class TriggerHeartbeatBody(BaseModel):
    extension_id: str


@app.get("/extension/heartbeats")
def extension_heartbeats(_: None = Depends(require_auth)) -> dict:
    heartbeats = get_all_heartbeat_status()
    return {"heartbeats": heartbeats}


@app.delete("/extension/heartbeat/{extension_id}")
def extension_heartbeat_delete(
    extension_id: str,
    _: None = Depends(require_auth),
) -> dict:
    ext_delete_heartbeats(extension_id)
    log_msg("HEARTBEAT", f"heartbeats deleted for {extension_id} (admin)")
    return {"success": True}


@app.post("/admin/trigger-heartbeat")
def admin_trigger_heartbeat(
    body: TriggerHeartbeatBody,
    _: None = Depends(require_auth),
) -> dict:
    record_heartbeat(body.extension_id)
    log_msg("HEARTBEAT", f"manual heartbeat triggered for {body.extension_id} (admin)")
    return {"success": True, "extension_id": body.extension_id}


@app.get("/admin/next-stale-check")
def admin_next_stale_check(_: None = Depends(require_auth)) -> dict:
    if _scheduler is None:
        return {"next_run": None}
    job = _scheduler.get_job("heartbeat_monitor")
    if job is None or job.next_run_time is None:
        return {"next_run": None}
    return {"next_run": job.next_run_time.isoformat()}


@app.get("/classify/url-fused")
def classify_url_fused(url: AnyHttpUrl = Query(...)) -> dict[str, Any]:
    url_str: str = str(url)
    hostname: str = parse_hostname(url_str)

    # Rate limit: 10req/min per hostname (cegah spam refresh loading page)
    if cache_available():
        rl_key: str = f"rate:fused:{hostname}"
        count = cache_incr(rl_key, ttl=60)
        if count > 10:
            log_msg("API", f"RATE LIMITED hostname={hostname}")
            cached = cache_get(f"fused:{cache_key(hostname)}")
            if cached:
                result = json.loads(cached)
                enrich_screenshot_url(result)
                result["from_cache"] = True
                return result
            log_msg("API", "raising 429")
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "rate_limited",
                    "message": "Too many requests. Please wait before retrying.",
                },
            )

    listed: str | None = list_check(hostname)
    if listed == "whitelist":
        return {
            "url": url_str,
            "category": "non-gambling",
            "gambling_score": 0.0,
            "text_score": 0.0,
            "image_score": None,
            "fusion_alpha": 0.0,
            "screenshot_url": None,
            "screenshot_status": "bypass_list",
            "from_cache": False,
            "from_list": "whitelist",
        }
    if listed == "blacklist":
        return {
            "url": url_str,
            "category": "gambling",
            "gambling_score": 1.0,
            "text_score": 1.0,
            "image_score": None,
            "fusion_alpha": 0.0,
            "screenshot_url": None,
            "screenshot_status": "bypass_list",
            "from_cache": False,
            "from_list": "blacklist",
        }

    key: str = f"fused:{cache_key(hostname)}"

    if cache_available():
        cached = cache_get(key)
        if cached is not None:
            result = json.loads(cached)
            enrich_screenshot_url(result)
            result["from_cache"] = True
            return result

    if is_ip(hostname):
        path: str = urlparse(url_str).path
        if not path or path == "/":
            return {
                "url": url_str,
                "category": "bare-ip",
                "gambling_score": 0.0,
                "text_score": 0.0,
                "image_score": None,
                "fusion_alpha": 0.0,
                "screenshot_url": None,
                "screenshot_status": "bypass_bare_ip",
                "from_cache": False,
            }

    if not model_loaded():
        log_msg("API", "model not loaded, raising 503")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "model_not_loaded",
                "message": "Models not available. Train and save to backend/model/bin/",
            },
        )

    result = infer_fused(url_str)

    if cache_available():
        cached_result = dict(result)
        cached_result.pop("screenshot_url", None)
        cache_setex(key, json.dumps(cached_result))

    result["from_cache"] = False
    return result


@app.get("/classify/result")
def classify_result(url: AnyHttpUrl = Query(...)) -> dict[str, Any]:
    url_str: str = str(url)
    hostname: str = parse_hostname(url_str)

    listed: str | None = list_check(hostname)
    if listed == "whitelist":
        return {
            "url": url_str,
            "status": "classified",
            "category": "non-gambling",
            "gambling_score": 0.0,
            "text_score": 0.0,
            "image_score": None,
            "fusion_alpha": None,
            "screenshot_url": None,
            "screenshot_status": None,
            "from_list": "whitelist",
            "from_cache": False,
        }
    if listed == "blacklist":
        return {
            "url": url_str,
            "status": "classified",
            "category": "gambling",
            "gambling_score": 1.0,
            "text_score": 1.0,
            "image_score": None,
            "fusion_alpha": None,
            "screenshot_url": None,
            "screenshot_status": None,
            "from_list": "blacklist",
            "from_cache": False,
        }

    if cache_available():
        fused_key = f"fused:{cache_key(hostname)}"
        cached = cache_get(fused_key)
        if cached:
            r = json.loads(cached)
            enrich_screenshot_url(r)
            return {
                "url": url_str,
                "status": "classified",
                "category": r["category"],
                "gambling_score": r.get("gambling_score", 0.0),
                "text_score": r.get("text_score"),
                "image_score": r.get("image_score"),
                "fusion_alpha": r.get("fusion_alpha"),
                "screenshot_url": r.get("screenshot_url"),
                "screenshot_status": r.get("screenshot_status"),
                "from_list": r.get("from_list", ""),
                "from_cache": True,
            }

    return {
        "url": url_str,
        "status": "not_classified",
        "category": None,
        "gambling_score": None,
        "text_score": None,
        "image_score": None,
        "fusion_alpha": None,
        "screenshot_url": None,
        "screenshot_status": None,
        "from_list": "",
        "from_cache": False,
    }


@app.get("/cache")
def list_cache(
    limit: int = Query(50, ge=1, le=200),
    _: None = Depends(require_auth),
) -> dict[str, object]:
    return {"entries": cache_scan(limit)}


@app.delete("/cache")
def delete_all_cache(_: None = Depends(require_auth)) -> dict[str, object]:
    deleted: int = cache_flush()
    log_msg("CACHE", f"flushed {deleted} entries")
    return {"status": "ok", "deleted": deleted}


@app.delete("/cache/{key:path}")
def delete_cache_entry(
    key: str,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    cache_delete(key)
    log_msg("CACHE", f"deleted key={key}")
    return {"status": "ok"}


class ReportBody(BaseModel):
    url: str
    gambling_score: float


@app.post("/report/false-positive")
def report_false_positive(body: ReportBody, request: Request) -> dict[str, Any]:
    client_ip: str = request.client.host if request.client else "unknown"
    hostname: str = parse_hostname(body.url)

    listed: str | None = list_check(hostname)
    if listed == "blacklist":
        log_msg("REPORT", f"rejected (blacklisted) from {client_ip} | {body.url}")
        raise HTTPException(
            status_code=400,
            detail={
                "error": "already_blacklisted",
                "message": "This URL is already blacklisted by admin.",
            },
        )
    if listed == "whitelist":
        log_msg("REPORT", f"rejected (whitelisted) from {client_ip} | {body.url}")
        raise HTTPException(
            status_code=400,
            detail={
                "error": "already_whitelisted",
                "message": "This URL is already whitelisted by admin.",
            },
        )

    if not cache_available():
        log_msg("REPORT", f"rejected (no cache) from {client_ip}")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "cache_unavailable",
                "message": "Cache required for report validation. Try again later.",
            },
        )

    cached = cache_get(f"fused:{cache_key(hostname)}")
    if cached is None:
        log_msg("REPORT", f"rejected (not classified) from {client_ip} | {body.url}")
        raise HTTPException(
            status_code=400,
            detail={
                "error": "not_classified",
                "message": (
                    "This URL has not been classified yet. "
                    "Visit it first via the browser."
                ),
            },
        )

    count: int = cache_incr(f"report:ip:{client_ip}", ttl=3600)
    if count > 5:
        log_msg("REPORT", f"rate limited from {client_ip} (count={count})")
        raise HTTPException(
            status_code=429,
            detail={
                "status": "rate_limited",
                "message": "Too many reports. Try again later.",
                "retry_after": 3600,
            },
        )

    save_report(body.url, body.gambling_score, client_ip)
    log_msg(
        "REPORT",
        f"false positive from {client_ip} | url={body.url}"
        f" | score={body.gambling_score}",
    )
    return {"status": "ok", "message": "Report saved"}


@app.get("/reports")
def list_reports(
    _: None = Depends(require_auth),
) -> dict[str, object]:
    return {
        "groups": get_grouped_reports(),
        "stats": get_report_stats(),
    }


class ListBody(BaseModel):
    hostname: str


@app.delete("/reports/by-hostname/{hostname}")
def delete_reports_by_hostname_endpoint(
    hostname: str,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    h = parse_hostname(hostname)
    reports_delete_by_host(h)
    log_msg("REPORT", f"deleted by hostname={h}")
    return {"status": "ok"}


@app.delete("/reports/{report_id}")
def delete_report_endpoint(
    report_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not reports_delete(report_id):
        log_msg("REPORT", f"delete failed: report_id={report_id} not found")
        raise HTTPException(status_code=404, detail="Report not found")
    log_msg("REPORT", f"deleted report_id={report_id}")
    return {"status": "ok"}


@app.get("/blacklist")
def get_blacklist(_: None = Depends(require_auth)) -> dict[str, object]:
    return {"entries": list_get("blacklist")}


@app.post("/blacklist")
def add_blacklist(
    body: ListBody,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    hostname = parse_hostname(body.hostname)
    entry = list_add(hostname, "blacklist")
    if entry is None:
        log_msg("LIST", f"blacklist conflict: {hostname}")
        raise HTTPException(
            status_code=409, detail="Hostname already in blacklist/whitelist"
        )
    cache_delete(f"fused:{cache_key(hostname)}")
    reports_delete_by_host(hostname)
    log_msg("LIST", f"added blacklist: {hostname}")
    return {"entry": entry}


@app.delete("/blacklist/{entry_id}")
def delete_blacklist(
    entry_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not list_remove(entry_id):
        log_msg("LIST", f"delete blacklist failed: id={entry_id} not found")
        raise HTTPException(status_code=404, detail="Entry not found")
    log_msg("LIST", f"deleted blacklist id={entry_id}")
    return {"status": "ok"}


@app.get("/whitelist")
def get_whitelist(_: None = Depends(require_auth)) -> dict[str, object]:
    return {"entries": list_get("whitelist")}


@app.post("/whitelist")
def add_whitelist(
    body: ListBody,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    hostname = parse_hostname(body.hostname)
    entry = list_add(hostname, "whitelist")
    if entry is None:
        log_msg("LIST", f"whitelist conflict: {hostname}")
        raise HTTPException(
            status_code=409, detail="Hostname already in whitelist/blacklist"
        )
    cache_delete(f"fused:{cache_key(hostname)}")
    reports_delete_by_host(hostname)
    log_msg("LIST", f"added whitelist: {hostname}")
    return {"entry": entry}


@app.delete("/whitelist/{entry_id}")
def delete_whitelist(
    entry_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not list_remove(entry_id):
        log_msg("LIST", f"delete whitelist failed: id={entry_id} not found")
        raise HTTPException(status_code=404, detail="Entry not found")
    log_msg("LIST", f"deleted whitelist id={entry_id}")
    return {"status": "ok"}


@app.get("/logs")
def list_logs(
    tag: str = Query(None),
    _: None = Depends(require_auth),
) -> dict[str, object]:
    return {"entries": logs_get(tag)}


@app.delete("/logs")
def delete_logs(_: None = Depends(require_auth)) -> dict[str, object]:
    logs_clear()
    log_msg("API", "logs cleared")
    return {"status": "ok"}


@app.get("/settings")
def get_settings(_: None = Depends(require_auth)) -> dict:
    return settings_get()


@app.put("/settings")
def update_settings(
    body: dict,
    _: None = Depends(require_auth),
) -> dict:
    allowed = {
        "bypass_text_enabled",
        "multipage_enabled",
        "debug_logging_enabled",
        "cache_ttl_hours",
        "stale_hours",
        "stale_check_interval_minutes",
        "auto_heartbeat_on_setup",
    }
    updates = {k: v for k, v in body.items() if k in allowed}
    if not updates:
        raise HTTPException(status_code=400, detail="No valid fields provided")
    log_msg("SETTINGS", f"update: {json.dumps(updates)}")
    result = settings_save(updates)
    if "stale_check_interval_minutes" in updates and _scheduler is not None:
        interval = updates["stale_check_interval_minutes"]
        try:
            _scheduler.reschedule_job(
                "heartbeat_monitor", trigger="interval", minutes=interval
            )
            log_msg("SETTINGS", f"rescheduled heartbeat monitor to every {interval}m")
        except Exception as exc:
            log_msg("WARN", f"failed to reschedule: {exc}")
    return result


@app.post("/admin/trigger-stale-check")
def admin_trigger_stale_check(_: None = Depends(require_auth)) -> dict:
    from utils.extensions import get_stale_extensions

    hours = settings_get().get("stale_hours", 2)
    stale = get_stale_extensions(hours)
    count = len(stale)
    log_msg("API", f"admin trigger stale check: {count} stale (threshold={hours}h)")
    _check_stale_heartbeats()
    return {"success": True, "stale_count": count, "alerts_sent": count}
