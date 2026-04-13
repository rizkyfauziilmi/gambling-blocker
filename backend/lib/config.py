from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class ScraperConfig:
    """Configuration untuk scraper dengan path relative ke backend directory."""

    output_dir: Path = field(
        default_factory=lambda: Path(__file__).parent.parent / "logs"
    )
    dataset_dir: Path = field(
        default_factory=lambda: Path(__file__).parent.parent / "model" / "dataset"
    )
    csv_filename: str = "raw_dataset.csv"
    jsonl_filename: str = "raw_dataset.jsonl"
    log_filename: str = "scraper.log"

    # Request settings
    timeout: int = 15
    delay_between_requests: float = 2.0  # detik (per worker)
    max_retries: int = 3
    retry_backoff: float = 2.0  # faktor pengali backoff

    # Concurrency
    max_workers: int = 3

    # Tag yang dibuang dari body text
    removed_tags: tuple = ("script", "style", "nav", "header", "footer", "aside")

    headers: dict = field(
        default_factory=lambda: {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
    )


# Global config instance
SCRAPER_CONFIG = ScraperConfig()
