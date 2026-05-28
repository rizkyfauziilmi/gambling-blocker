import json
from typing import Any
from urllib.parse import urlparse

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import AnyHttpUrl

from utils.cache import get as cache_get
from utils.cache import is_available as cache_available
from utils.cache import setex as cache_setex
from utils.helpers import cache_key, is_ip, resolve_ips
from utils.model import infer
from utils.model import is_loaded as model_loaded

app: FastAPI = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


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
                "category": "non-gambling",
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
