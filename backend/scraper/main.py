import pandas as pd
from .extractor import process_urls
from constant.link import GAMBLING_SITES, NON_GAMBLING_SITES
import datetime

if __name__ == "__main__":
    print("Starting data collection...")
    
    safe_data = process_urls(NON_GAMBLING_SITES, label=0)
    gambling_data = process_urls(GAMBLING_SITES, label=1)
    
    all_data = safe_data + gambling_data
    
    df = pd.DataFrame(all_data)
    
    filename = f"domain_classification_dataset_{datetime.datetime.now().strftime('%Y%m%d')}.csv"
    path = f"data/{filename}"
    df.to_csv(path, index=False)
    
    print(f"\n[+] Extraction complete. Saved {len(df)} records to {filename}")