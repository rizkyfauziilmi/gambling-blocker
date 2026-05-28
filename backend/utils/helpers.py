import ipaddress
import re
import socket
from urllib.parse import unquote


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


def prepare_url_for_cnn(url: str) -> str:
    url = unquote(url).lower()
    url = re.sub(r"https?:\/\/", "", url)
    url = re.sub(r"[?#].*$", "", url)
    return url.strip()
