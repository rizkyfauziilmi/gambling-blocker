import re
from urllib.parse import unquote, urlparse

from fastapi import HTTPException


def parse_hostname(raw: str) -> str:
    raw = raw.strip().lower()
    if not raw:
        raise HTTPException(status_code=400, detail="Hostname is required")
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    hostname = urlparse(raw).hostname
    if not hostname:
        raise HTTPException(status_code=400, detail="Invalid URL or hostname")
    if "." not in hostname:
        raise HTTPException(status_code=400, detail="Invalid hostname format")
    return hostname


def cache_key(hostname: str) -> str:
    return f"domain:{hostname}"


def clean_url(url: str) -> str:
    parsed = urlparse(url)
    url = parsed.netloc + parsed.path
    url = unquote(url).lower()
    url = re.sub(r"https?:\/\/", "", url)
    url = re.sub(r"[-_/]", " ", url)
    url = re.sub(r"[^a-zA-Z0-9\s]", " ", url)
    url = re.sub(r"(\d)([a-z])", r"\1 \2", url)
    url = re.sub(r"([a-z])(\d)", r"\1 \2", url)
    url = re.sub(r"\s+", " ", url).strip()

    return url
