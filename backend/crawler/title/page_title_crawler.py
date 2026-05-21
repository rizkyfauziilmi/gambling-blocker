import pandas as pd
import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

# 1. Load dataset awal
df = pd.read_csv("backend/model/dataset/dataset_crawl_070526.csv")


def get_page_title(url):
    try:
        # Timeout penting agar script tidak berhenti lama di situs yang mati
        response = requests.get(
            url,
            timeout=15,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
            },
        )
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, "html.parser")
            if soup.title and soup.title.string:
                return soup.title.string.strip()
        return "no_title_found"  # Strategi Handling Missing Value
    except Exception:
        # Fallback jika situs mati (404/Timeout)
        return "unknown_site"


# 2. Proses secara paralel
# Sesuaikan max_workers dengan kemampuan internet/CPU (misal: 20-50)
urls = df["Url"].tolist()
titles = []

print("Memulai pengambilan Page Title secara paralel...")
with ThreadPoolExecutor(max_workers=30) as executor:
    # Menggunakan tqdm untuk melihat progress bar
    titles = list(tqdm(executor.map(get_page_title, urls), total=len(urls)))

# 3. Masukkan hasil ke kolom baru dan simpan dataset
df["Page_Title"] = titles
df.to_csv("dataset_multimodal_new.csv", index=False)

print("Selesai! Dataset baru disimpan sebagai 'dataset_multimodal_new.csv'")
