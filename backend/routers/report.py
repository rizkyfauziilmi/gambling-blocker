from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from utils.cache import get as cache_get
from utils.cache import incr as cache_incr
from utils.cache import is_available as cache_available
from utils.helpers import cache_key, parse_hostname
from utils.lists import check_hostname as list_check
from utils.logger import log as log_msg
from utils.reports import save_report

router = APIRouter(tags=["report"])


class ReportBody(BaseModel):
    url: str
    gambling_score: float


@router.post("/report")
def report_false_positive(body: ReportBody, request: Request) -> dict:
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
