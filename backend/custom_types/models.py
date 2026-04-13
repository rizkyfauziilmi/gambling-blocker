from dataclasses import dataclass


@dataclass
class ScrapedRecord:
    """Data model untuk record hasil scraping satu URL."""

    timestamp: str
    url: str
    domain: str
    domain_ext: str
    http_status: int
    raw_title: str
    raw_meta_desc: str
    label: int  # 1 = Gambling, 0 = Non-Gambling


# Field names untuk CSV export
CSV_FIELDS = (
    "timestamp",
    "url",
    "domain",
    "domain_ext",
    "http_status",
    "raw_title",
    "raw_meta_desc",
    "label",
)
