import csv
from urllib.parse import urlparse
from constant.link import GAMBLING_SITES, NON_GAMBLING_SITES


def extract_domain(url):
    return urlparse(url).netloc.lower()


def normalize_url(url):
    return url.strip().lower()


seen_urls = set()
seen_domains = set()

rows = []
current_id = 1


def add_row(url, label):
    global current_id

    norm_url = normalize_url(url)
    domain = extract_domain(norm_url)

    # check duplicates
    if norm_url in seen_urls:
        raise ValueError(f"Duplicate URL detected: {url}")
    if domain in seen_domains:
        raise ValueError(f"Duplicate domain detected: {domain}")

    seen_urls.add(norm_url)
    seen_domains.add(domain)

    rows.append([current_id, domain, url, label])
    current_id += 1


# gambling
for url in GAMBLING_SITES:
    add_row(url, "gambling")

# non-gambling
for url in NON_GAMBLING_SITES:
    add_row(url, "non-gambling")

# write CSV
with open("datasets/domain.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["id", "domain", "url", "label"])
    writer.writerows(rows)

print("CSV file 'domain.csv' created successfully.")
