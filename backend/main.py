import json
import os
from typing import Any
from urllib.parse import urlparse

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import AnyHttpUrl, BaseModel

from utils.cache import delete as cache_delete
from utils.cache import flush_cache as cache_flush
from utils.cache import get as cache_get
from utils.cache import incr as cache_incr
from utils.cache import is_available as cache_available
from utils.cache import scan as cache_scan
from utils.cache import setex as cache_setex
from utils.helpers import cache_key, is_ip, parse_hostname, resolve_ips
from utils.lists import add_entry as list_add
from utils.lists import check_hostname as list_check
from utils.lists import get_entries as list_get
from utils.lists import remove_entry as list_remove
from utils.model import infer, infer_fused
from utils.model import is_loaded as model_loaded
from utils.reports import delete_report as reports_delete
from utils.reports import delete_reports_by_hostname as reports_delete_by_host
from utils.reports import get_grouped_reports, get_report_stats, save_report

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
    if credentials.username != DASHBOARD_USER or credentials.password != DASHBOARD_PASS:
        raise HTTPException(status_code=401)


@app.get("/")
def root() -> dict[str, str]:
    return {"service": "url gambling classifier", "status": "running"}


@app.get("/classify/url")
def classify_url(url: AnyHttpUrl = Query(...)) -> dict[str, Any]:
    url_str: str = str(url)
    hostname: str = parse_hostname(url_str)

    listed: str | None = list_check(hostname)
    if listed == "whitelist":
        return {
            "url": url_str,
            "category": "non-gambling",
            "gambling_score": 0,
            "resolved_ips": [],
            "from_cache": False,
            "from_list": "whitelist",
        }
    if listed == "blacklist":
        return {
            "url": url_str,
            "category": "gambling",
            "gambling_score": 1.0,
            "resolved_ips": resolve_ips(hostname),
            "from_cache": False,
            "from_list": "blacklist",
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
                "message": (
                    "CNN model not available. "
                    "Train and save model files to backend/model/bin/"
                ),
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


@app.get("/classify/url-fused")
def classify_url_fused(url: AnyHttpUrl = Query(...)) -> dict[str, Any]:
    url_str: str = str(url)
    hostname: str = parse_hostname(url_str)

    print(f"[API] /classify/url-fused called | url={url_str} | hostname={hostname}")

    # Rate limit: 10req/min per hostname (cegah spam refresh loading page)
    if cache_available():
        rl_key: str = f"rate:fused:{hostname}"
        count = cache_incr(rl_key, ttl=60)
        print(f"[API] rate limit count={count}")
        if count > 10:
            print(f"[API] RATE LIMITED hostname={hostname}")
            cached = cache_get(f"fused:{cache_key(hostname)}")
            if cached:
                result = json.loads(cached)
                result["from_cache"] = True
                print(f"[API] served from fallback cache | result={json.dumps(result)}")
                return result
            print(f"[API] raising 429")
            raise HTTPException(
                status_code=429,
                detail={
                    "error": "rate_limited",
                    "message": "Too many requests. Please wait before retrying.",
                },
            )

    listed: str | None = list_check(hostname)
    print(f"[API] list_check={listed}")
    if listed == "whitelist":
        print(f"[API] whitelist, returning safe")
        return {
            "url": url_str,
            "category": "non-gambling",
            "gambling_score": 0.0,
            "text_score": 0.0,
            "image_score": None,
            "fusion_alpha": 0.0,
            "screenshot_url": None,
            "screenshot_status": "bypass_list",
            "resolved_ips": [],
            "from_cache": False,
            "from_list": "whitelist",
        }
    if listed == "blacklist":
        print(f"[API] blacklist, returning gambling")
        return {
            "url": url_str,
            "category": "gambling",
            "gambling_score": 1.0,
            "text_score": 1.0,
            "image_score": None,
            "fusion_alpha": 0.0,
            "screenshot_url": None,
            "screenshot_status": "bypass_list",
            "resolved_ips": resolve_ips(hostname),
            "from_cache": False,
            "from_list": "blacklist",
        }

    key: str = f"fused:{cache_key(hostname)}"

    if cache_available():
        cached = cache_get(key)
        if cached is not None:
            result = json.loads(cached)
            result["from_cache"] = True
            print(f"[API] served from fused cache | result={json.dumps(result)}")
            return result

    if not model_loaded():
        print(f"[API] model not loaded, raising 503")
        raise HTTPException(
            status_code=503,
            detail={
                "error": "model_not_loaded",
                "message": "Models not available. Train and save to backend/model/bin/",
            },
        )

    if is_ip(hostname):
        path: str = urlparse(url_str).path
        if not path or path == "/":
            print(f"[API] bare ip, returning safe")
            return {
                "url": url_str,
                "category": "bare-ip",
                "gambling_score": 0.0,
                "text_score": 0.0,
                "image_score": None,
                "fusion_alpha": 0.0,
                "screenshot_url": None,
                "screenshot_status": "bypass_bare_ip",
                "resolved_ips": [hostname],
                "from_cache": False,
            }

    print(f"[API] running infer_fused...")
    result = infer_fused(url_str)
    print(f"[API] infer_fused result={json.dumps(result)}")

    if result["category"] == "gambling":
        ips: list[str] = resolve_ips(hostname)
        result["resolved_ips"] = ips
    else:
        result["resolved_ips"] = []

    if cache_available():
        print(f"[API] writing to cache key={key}")
        cache_setex(key, json.dumps(result))
        if result["category"] == "gambling":
            for ip in result["resolved_ips"]:
                ip_cache: dict[str, Any] = {
                    "url": result["url"],
                    "category": "gambling",
                    "gambling_score": result["gambling_score"],
                    "resolved_ips": [ip],
                }
                cache_setex(f"fused:ip:{ip}", json.dumps(ip_cache))
                print(f"[API] wrote ip cache for {ip}")

    result["from_cache"] = False
    print(f"[API] returning final result={json.dumps(result)}")
    return result


@app.get("/classify/result")
def classify_result(url: AnyHttpUrl = Query(...)) -> dict[str, Any]:
    url_str: str = str(url)
    hostname: str = parse_hostname(url_str)

    print(f"[API] /classify/result called | url={url_str} | hostname={hostname}")

    listed: str | None = list_check(hostname)
    print(f"[API] result list_check={listed}")
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
            "resolved_ips": [],
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
            "resolved_ips": resolve_ips(hostname),
        }

    if cache_available():
        fused_key = f"fused:{cache_key(hostname)}"
        text_key = cache_key(hostname)
        cached = cache_get(fused_key)
        print(f"[API] result cache: fused={fused_key} found={cached is not None}")
        if not cached:
            cached = cache_get(text_key)
            print(f"[API] result cache: text={text_key} found={cached is not None}")
        if cached:
            r = json.loads(cached)
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
                "resolved_ips": r.get("resolved_ips", []),
            }

    print(f"[API] result: no cache found, returning not_classified")
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
        "resolved_ips": [],
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
    return {"status": "ok", "deleted": deleted}


@app.delete("/cache/{key:path}")
def delete_cache_entry(
    key: str,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    cache_delete(key)
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
        raise HTTPException(
            status_code=400,
            detail={
                "error": "already_blacklisted",
                "message": "This URL is already blacklisted by admin.",
            },
        )
    if listed == "whitelist":
        raise HTTPException(
            status_code=400,
            detail={
                "error": "already_whitelisted",
                "message": "This URL is already whitelisted by admin.",
            },
        )

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
                "message": (
                    "This URL has not been classified yet. "
                    "Visit it first via the browser."
                ),
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
    return {"status": "ok"}


@app.delete("/reports/{report_id}")
def delete_report_endpoint(
    report_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not reports_delete(report_id):
        raise HTTPException(status_code=404, detail="Report not found")
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
        raise HTTPException(
            status_code=409, detail="Hostname already in blacklist/whitelist"
        )
    cache_delete(cache_key(hostname))
    reports_delete_by_host(hostname)
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
    hostname = parse_hostname(body.hostname)
    entry = list_add(hostname, "whitelist")
    if entry is None:
        raise HTTPException(
            status_code=409, detail="Hostname already in whitelist/blacklist"
        )
    cache_delete(cache_key(hostname))
    reports_delete_by_host(hostname)
    return {"entry": entry}


@app.delete("/whitelist/{entry_id}")
def delete_whitelist(
    entry_id: int,
    _: None = Depends(require_auth),
) -> dict[str, object]:
    if not list_remove(entry_id):
        raise HTTPException(status_code=404, detail="Entry not found")
    return {"status": "ok"}
