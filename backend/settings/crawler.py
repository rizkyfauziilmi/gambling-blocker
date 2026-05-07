import re

CRAWLER_SETTINGS = {
    "max_pages_per_domain": 200,
    "delay_seconds": 1,
    "request_timeout": 10,
    "respect_robots_txt": False,
    "max_workers": 20,
    "output_csv": "crawler/bin/dataset_crawl.csv",
    "output_json": "crawler/bin/dataset_crawl.json",
    "log_file": "logs/crawler.log",
    "checkpoint_file": "logs/checkpoint.json",
    "domain_timeout": 300,  # detik sebelum domain dianggap macet
    "flush_every_n": 10,  # flush buffer setiap N record
    "flush_every_s": 30.0,  # atau setiap N detik, mana yang lebih dulu
}

CRAWLER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}

CSV_FIELDNAMES = ["Webpage_id", "Domain", "Url", "Tag", "Crawled_at"]

SKIP_PATTERNS = re.compile(
    r"(mailto:|javascript:|tel:|#|/logout|/cdn-cgi/|/wp-json/|/feed/|\.rss$|/sitemap|\.xml$)"
)

SKIP_EXTENSIONS = {
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".webp",  # gambar
    ".pdf",
    ".zip",
    ".rar",
    ".exe",  # dokumen & arsip
    ".mp4",
    ".mp3",  # media
    ".css",
    ".js",  # aset statis
    ".ico",
    ".woff",
    ".woff2",
    ".ttf",  # font & ikon
    ".xml",
    ".json",  # data terstruktur
}

MAX_URL_LENGTH = 300
