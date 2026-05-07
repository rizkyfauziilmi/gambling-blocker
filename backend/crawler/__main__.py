from colorama import init
from crawler.links import DOMAINS_TO_CRAWL

from crawler.crawler import run_crawler
from crawler.logger import crawler_log_block
from crawler.storage import export, load_existing_dataset
from settings.crawler import CRAWLER_SETTINGS
from utils.crawler import validate_domains

init(autoreset=True)

if __name__ == "__main__":
    crawler_log_block(
        "",
        "=" * 60,
        "  Multi-Domain Web Crawler & Dataset Builder  [Multithreaded]",
        f"  Domain  : {len(DOMAINS_TO_CRAWL)}",
        f"  Worker  : {CRAWLER_SETTINGS['max_workers']}",
        "=" * 60,
    )

    validate_domains(DOMAINS_TO_CRAWL)
    existing_df = load_existing_dataset(CRAWLER_SETTINGS["output_csv"])

    # Jalankan crawler
    run_crawler(DOMAINS_TO_CRAWL, existing_df)

    # Export statistik akhir
    export()
