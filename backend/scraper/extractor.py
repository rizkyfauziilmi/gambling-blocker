import urllib.parse
import socket
import ssl
import whois
import dns.resolver
from ipwhois import IPWhois
import uuid

def get_domain(url: str) -> str:
    """Extracts the base domain from a URL."""
    parsed = urllib.parse.urlparse(url)
    domain = parsed.netloc
    if domain.startswith('www.'):
        domain = domain[4:]
    return domain

def get_whois(domain: str) -> str:
    try:
        w = whois.whois(domain)
        return str(w.text) if w.text else str(w)
    except Exception as e:
        return f"Error: {e}"

def get_dns_records(domain: str) -> str:
    records = {}
    record_types = ['A', 'NS', 'MX', 'TXT']
    for qtype in record_types:
        try:
            answers = dns.resolver.resolve(domain, qtype)
            records[qtype] = [rdata.to_text() for rdata in answers]
        except Exception:
            records[qtype] = []
    return str(records)

def get_ip_and_asn(domain: str) -> tuple[str, str]:
    ip_address = None
    asn_info = None
    try:
        ip_address = socket.gethostbyname(domain)
        # Fetch ASN beradasarkan IP
        obj = IPWhois(ip_address)
        res = obj.lookup_rdap(depth=1)
        asn_info = f"ASN: {res.get('asn')} | Country: {res.get('network', {}).get('country')}"
    except Exception as e:
        asn_info = f"Error: {e}"
    
    return ip_address, asn_info

def get_ssl_info(domain):
    try:
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE 
        
        # Paksa penggunaan IPv4 untuk menghindari error "No route to host" pada beberapa domain yang mungkin memiliki masalah dengan IPv6
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5.0)
        
        with ctx.wrap_socket(s, server_hostname=domain) as ss:
            ss.connect((domain, 443))
            cert = ss.getpeercert(binary_form=True)
            
            from cryptography import x509
            from cryptography.hazmat.backends import default_backend
            x509_cert = x509.load_der_x509_certificate(cert, default_backend())
            
            issuer = x509_cert.issuer.rfc4514_string()
            
            expiry = x509_cert.not_valid_after_utc.strftime("%Y-%m-%d")
            return f"Issuer: {issuer} | Expires: {expiry}"
            
    except Exception as e:
        return f"Error/No SSL: {e}"

def process_url(url: str, label: int) -> dict:
    print(f"[*] Processing {url}...")
    domain = get_domain(url)
    
    ip_address, asn = get_ip_and_asn(domain)
    whois_raw = get_whois(domain)
    dns_records = get_dns_records(domain)
    ssl_info = get_ssl_info(domain)
    
    return {
        "id": str(uuid.uuid4()),
        "original_url": url,
        "domain": domain,
        "whois_raw": whois_raw,
        "dns_records": dns_records,
        "ip_address": ip_address,
        "asn": asn,
        "ssl_info": ssl_info,
        "label": label # 0 untuk Non-Gambling, 1 untuk Gambling
    }

def process_urls(urls: list[str], label: int) -> list[dict]:
    dataset = []
    for url in urls:
        data_row = process_url(url, label)
        dataset.append(data_row)
        
    return dataset