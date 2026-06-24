import json

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel

from routers.auth import require_auth
from utils.cache import delete as cache_delete
from utils.cache import flush_cache as cache_flush
from utils.cache import scan as cache_scan
from utils.extensions import get_stale_extensions, record_heartbeat
from utils.helpers import cache_key, parse_hostname
from utils.lists import add_entry as list_add
from utils.lists import get_entries as list_get
from utils.lists import remove_entry as list_remove
from utils.logger import clear as logs_clear
from utils.logger import get_logs as logs_get
from utils.logger import log as log_msg
from utils.reports import delete_report as reports_delete
from utils.reports import delete_reports_by_hostname as reports_delete_by_host
from utils.reports import get_grouped_reports, get_report_stats
from utils.settings import get as settings_get
from utils.settings import save as settings_save

router = APIRouter(dependencies=[Depends(require_auth)], tags=["dashboard"])


class ListBody(BaseModel):
    hostname: str


# ---- Blacklist / Whitelist (generic) ----


@router.get("/lists/{list_type}")
def get_list(list_type: str) -> dict:
    if list_type not in ("blacklist", "whitelist"):
        raise HTTPException(status_code=400, detail="Invalid list_type")
    return {"entries": list_get(list_type)}


@router.post("/lists/{list_type}")
def add_to_list(list_type: str, body: ListBody) -> dict:
    if list_type not in ("blacklist", "whitelist"):
        raise HTTPException(status_code=400, detail="Invalid list_type")
    hostname = parse_hostname(body.hostname)
    entry = list_add(hostname, list_type)
    if entry is None:
        log_msg("LIST", f"{list_type} conflict: {hostname}")
        raise HTTPException(
            status_code=409, detail="Hostname already in blacklist/whitelist"
        )
    cache_delete(f"fused:{cache_key(hostname)}")
    reports_delete_by_host(hostname)
    log_msg("LIST", f"added {list_type}: {hostname}")
    return {"entry": entry}


@router.delete("/lists/{list_type}/{entry_id}")
def delete_from_list(list_type: str, entry_id: int) -> dict:
    if list_type not in ("blacklist", "whitelist"):
        raise HTTPException(status_code=400, detail="Invalid list_type")
    if not list_remove(entry_id):
        log_msg("LIST", f"delete {list_type} failed: id={entry_id} not found")
        raise HTTPException(status_code=404, detail="Entry not found")
    log_msg("LIST", f"deleted {list_type} id={entry_id}")
    return {"status": "ok"}


# ---- Reports ----


@router.get("/reports")
def list_reports() -> dict:
    return {
        "groups": get_grouped_reports(),
        "stats": get_report_stats(),
    }


@router.delete("/reports/by-hostname/{hostname}")
def delete_reports_by_hostname_endpoint(hostname: str) -> dict:
    h = parse_hostname(hostname)
    reports_delete_by_host(h)
    log_msg("REPORT", f"deleted by hostname={h}")
    return {"status": "ok"}


@router.delete("/reports/{report_id}")
def delete_report_endpoint(report_id: int) -> dict:
    if not reports_delete(report_id):
        log_msg("REPORT", f"delete failed: report_id={report_id} not found")
        raise HTTPException(status_code=404, detail="Report not found")
    log_msg("REPORT", f"deleted report_id={report_id}")
    return {"status": "ok"}


# ---- Cache ----


@router.get("/cache")
def list_cache(limit: int = Query(50, ge=1, le=200)) -> dict:
    return {"entries": cache_scan(limit)}


@router.delete("/cache")
def delete_all_cache() -> dict:
    deleted: int = cache_flush()
    log_msg("CACHE", f"flushed {deleted} entries")
    return {"status": "ok", "deleted": deleted}


@router.delete("/cache/{key:path}")
def delete_cache_entry(key: str) -> dict:
    cache_delete(key)
    log_msg("CACHE", f"deleted key={key}")
    return {"status": "ok"}


# ---- Logs ----


@router.get("/logs")
def list_logs(tag: str = Query(None)) -> dict:
    return {"entries": logs_get(tag)}


@router.delete("/logs")
def delete_logs() -> dict:
    logs_clear()
    log_msg("API", "logs cleared")
    return {"status": "ok"}


# ---- Settings ----


@router.get("/settings")
def get_settings() -> dict:
    return settings_get()


@router.put("/settings")
def update_settings(body: dict, request: Request) -> dict:
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
    if "stale_check_interval_minutes" in updates:
        scheduler = getattr(request.app.state, "scheduler", None)
        if scheduler is not None:
            interval = updates["stale_check_interval_minutes"]
            try:
                scheduler.reschedule_job(
                    "heartbeat_monitor", trigger="interval", minutes=interval
                )
                log_msg(
                    "SETTINGS", f"rescheduled heartbeat monitor to every {interval}m"
                )  # noqa: E501
            except Exception as exc:
                log_msg("WARN", f"failed to reschedule: {exc}")
    return result


# ---- Admin ----


class TriggerHeartbeatBody(BaseModel):
    extension_id: str


@router.post("/admin/trigger-heartbeat")
def admin_trigger_heartbeat(body: TriggerHeartbeatBody) -> dict:
    record_heartbeat(body.extension_id)
    log_msg("HEARTBEAT", f"manual heartbeat triggered for {body.extension_id} (admin)")
    return {"success": True, "extension_id": body.extension_id}


@router.get("/admin/next-stale-check")
def admin_next_stale_check(request: Request) -> dict:
    scheduler = getattr(request.app.state, "scheduler", None)
    if scheduler is None:
        return {"next_run": None}
    job = scheduler.get_job("heartbeat_monitor")
    if job is None or job.next_run_time is None:
        return {"next_run": None}
    return {"next_run": job.next_run_time.isoformat()}


@router.post("/admin/trigger-stale-check")
def admin_trigger_stale_check() -> dict:
    hours = settings_get().get("stale_hours", 2)
    stale = get_stale_extensions(hours)
    count = len(stale)
    log_msg("API", f"admin trigger stale check: {count} stale (threshold={hours}h)")
    # _check_stale_heartbeats is called indirectly via scheduler job;
    # import here to avoid circular dependency
    from main import _check_stale_heartbeats

    _check_stale_heartbeats()
    return {"success": True, "stale_count": count, "alerts_sent": count}
