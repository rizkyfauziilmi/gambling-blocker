import ipaddress
import re
import socket
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
    if "." not in hostname and not is_ip(hostname):
        raise HTTPException(status_code=400, detail="Invalid hostname format")
    return hostname


def is_ip(hostname: str) -> bool:
    try:
        ipaddress.ip_address(hostname)
        return True
    except ValueError:
        return False


def cache_key(hostname: str) -> str:
    return f"ip:{hostname}" if is_ip(hostname) else f"domain:{hostname}"


def resolve_ips(hostname: str) -> list[str]:
    try:
        addrs = socket.getaddrinfo(hostname, 80, type=socket.SOCK_STREAM)
        return list(set(str(addr[4][0]) for addr in addrs))
    except socket.gaierror:
        return []


def clean_url(url: str) -> str:
    url = unquote(url).lower()
    url = re.sub(r"https?:\/\/", "", url)
    url = re.sub(r"[-_/]", " ", url)
    url = re.sub(r"[^a-zA-Z0-9\s]", " ", url)
    url = re.sub(r"(\d)([a-z])", r"\1 \2", url)
    url = re.sub(r"([a-z])(\d)", r"\1 \2", url)
    url = re.sub(r"\s+", " ", url).strip()
    return url
