import requests
import csv
import os
import json
from datetime import datetime

def fetch_fda_data(output_dir="data/raw", state="NY", total_limit=1000):
    print("[INFO] Fetching FDA drug enforcement data...")

    URL = "https://api.fda.gov/drug/enforcement.json"
    batch_size = 100  # API max limit
    all_data = []

    os.makedirs(output_dir, exist_ok=True)

    for skip in range(0, total_limit, batch_size):
        params = {
            "search": f"state:{state}",
            "limit": batch_size,
            "skip": skip
        }

        response = requests.get(URL, params=params)
        if response.status_code != 200:
            print(f"[ERROR] API Error at skip={skip}: {response.status_code}")
            break

        batch = response.json().get("results", [])
        if not batch:
            print("[INFO] No more data.")
            break

        print(f"[INFO] Fetched {len(batch)} records (skip={skip})")
        all_data.extend(batch)

    if not all_data:
        print("[INFO] No data fetched.")
        return 0

    # Determine output path with today's date
    date_str = datetime.now().strftime("%Y%m%d")
    output_path = os.path.join(output_dir, f"fda_recall_{date_str}.csv")

    # Collect all keys for CSV headers
    headers = sorted(set().union(*(item.keys() for item in all_data)))

    # Write to CSV
    with open(output_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for item in all_data:
            writer.writerow({key: item.get(key, "") for key in headers})

    print(f"[SUCCESS] Saved {len(all_data)} rows to {output_path}")
    return len(all_data)

if __name__ == "__main__":
    fetch_fda_data()
