import json

from fastapi import APIRouter, HTTPException, Query
from pydantic import AnyHttpUrl

from utils.cache import get as cache_get
from utils.cache import incr as cache_incr
from utils.cache import is_available as cache_available
from utils.cache import setex as cache_setex
from utils.helpers import cache_key, parse_hostname
from utils.lists import check_hostname as list_check
from utils.logger import log as log_msg
from utils.model import infer_fused
from utils.model import is_loaded as model_loaded
from utils.storage import enrich_screenshot_url

router = APIRouter(tags=["classification"])


@router.get("/classify/url-fused")
def classify_url_fused(url: AnyHttpUrl = Query(...)) -> dict:
    url_str: str = str(url)
    hostname: str = parse_hostname(url_str)

    # Rate limit: 10req/min per hostname
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


@router.get("/classify/result")
def classify_result(url: AnyHttpUrl = Query(...)) -> dict:
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
