import json
import os
from typing import Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import AnyHttpUrl, BaseModel

from utils.cache import get as cache_get
from utils.cache import incr as cache_incr
from utils.cache import is_available as cache_available
from utils.cache import setex as cache_setex
from utils.helpers import cache_key, is_ip, resolve_ips
from utils.lists import add_entry as list_add
from utils.lists import check_hostname as list_check
from utils.lists import get_entries as list_get
from utils.lists import remove_entry as list_remove
from utils.model import infer
from utils.model import is_loaded as model_loaded
from utils.reports import get_all_reports, get_report_stats, save_report

app: FastAPI = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["*"],
)

security = HTTPBasic()
DASHBOARD_USER: str = os.getenv("DASHBOARD_USERNAME", "admin")
DASHBOARD_PASS: str = os.getenv("DASHBOARD_PASSWORD", "admin123")


def require_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    if (
        credentials.username != DASHBOARD_USER
        or credentials.password != DASHBOARD_PASS
    ):
        raise HTTPException(status_code=401)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "url gambling classifier", "status": "running"}


@app.get("/classify/url")
def classify_url(url: AnyHttpUrl = Query(...)) -> dict[str, Any]:
    url_str: str = str(url)
    hostname: str = urlparse(url_str).hostname or ""

    if not hostname:
        raise HTTPException(
            status_code=400, detail="Could not extract hostname from URL"
        )

    listed: str | None = list_check(hostname)
    if listed == "whitelist":
        return {
            "url": url_str,
            "category": "non-gambling",
            "gambling_score": 0,
            "resolved_ips": [],
            "from_cache": False,
        }
    if listed == "blacklist":
        return {
            "url": url_str,
            "category": "gambling",
            "gambling_score": 1.0,
            "resolved_ips": resolve_ips(hostname),
            "from_cache": False,
        }

    key: str = cache_key(hostname)

    if cache_available():
        cached = cache_get(key)
        if cached is not None:
            result = json.loads(cached)
            result["from_cache"] = True
            return result

    if not model_loaded():
        raise HTTPException(
            status_code=503,
            detail={
                "error": "model_not_loaded",
                "message": "CNN model not available. Train and save model files to backend/model/bin/",
            },
        )

    # BARE IP — no path, skip inference & caching
    if is_ip(hostname):
        path: str = urlparse(url_str).path
        if not path or path == "/":
            return {
                "url": url_str,
                "category": "bare-ip",
                "gambling_score": 0,
                "resolved_ips": [hostname],
                "from_cache": False,
            }

    result = infer(url_str)

    if result["category"] == "gambling":
        ips: list[str] = resolve_ips(hostname)
        result["resolved_ips"] = ips
    else:
        result["resolved_ips"] = []

    if cache_available():
        cache_setex(key, json.dumps(result))
        if result["category"] == "gambling":
            for ip in result["resolved_ips"]:
                ip_cache: dict[str, Any] = {
                    "url": result["url"],
                    "category": "gambling",
                    "gambling_score": result["gambling_score"],
                    "resolved_ips": [ip],
                }
                cache_setex(f"ip:{ip}", json.dumps(ip_cache))

    result["from_cache"] = False
    return result


class ReportBody(BaseModel):
    url: str
    gambling_score: float


@app.post("/report/false-positive")
def report_false_positive(body: ReportBody, request: Request) -> dict[str, Any]:
    client_ip: str = request.client.host if request.client else "unknown"
    hostname: str = urlparse(body.url).hostname or ""

    if not hostname:
        raise HTTPException(status_code=400, detail="Could not extract hostname from URL")

    if not cache_available():
        raise HTTPException(
            status_code=503,
            detail={
                "error": "cache_unavailable",
                "message": "Cache required for report validation. Try again later.",
            },
        )

    cached = cache_get(cache_key(hostname))
    if cached is None:
        raise HTTPException(
            status_code=400,
            detail={
                "error": "not_classified",
                "message": "This URL has not been classified yet. Visit it first via the browser.",
            },
        )

    count: int = cache_incr(f"report:ip:{client_ip}", ttl=3600)
    if count > 5:
        raise HTTPException(
            status_code=429,
            detail={
                "status": "rate_limited",
                "message": "Too many reports. Try again later.",
                "retry_after": 3600,
            },
        )

    save_report(body.url, body.gambling_score, client_ip)
    return {"status": "ok", "message": "Report saved"}


@app.get("/reports")
def list_reports(
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    _: None = Depends(require_auth),
) -> dict[str, object]:
    return {
        "reports": get_all_reports(limit, offset),
        "stats": get_report_stats(),
    }


class ListBody(BaseModel):
    hostname: str


@app.get("/blacklist")
def get_blacklist(_: None = Depends(require_auth)) -> dict[str, object]:
    return {"entries": list_get("blacklist")}


@app.post("/blacklist")
def add_blacklist(
    body: ListBody,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    entry = list_add(body.hostname.strip().lower(), "blacklist")
    if entry is None:
        raise HTTPException(
            status_code=409, detail="Hostname already in blacklist"
        )
    return {"entry": entry}


@app.delete("/blacklist/{entry_id}")
def delete_blacklist(
    entry_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not list_remove(entry_id):
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"status": "ok"}


@app.get("/whitelist")
def get_whitelist(_: None = Depends(require_auth)) -> dict[str, object]:
    return {"entries": list_get("whitelist")}


@app.post("/whitelist")
def add_whitelist(
    body: ListBody,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    entry = list_add(body.hostname.strip().lower(), "whitelist")
    if entry is None:
        raise HTTPException(
            status_code=409, detail="Hostname already in whitelist"
        )
    return {"entry": entry}


@app.delete("/whitelist/{entry_id}")
def delete_whitelist(
    entry_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not list_remove(entry_id):
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"status": "ok"}
