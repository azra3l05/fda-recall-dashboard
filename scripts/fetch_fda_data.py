import requests
import csv
import os
import json

def fetch_fda_data(output_path="data/fda_recall_2023.csv", state="NY", limit=100):
    URL = "https://api.fda.gov/drug/enforcement.json"
    params = {
        "search": f"state:{state}",
        "limit": limit
    }

    response = requests.get(URL, params=params)
    if response.status_code != 200:
        print(f"[ERROR] API Error: {response.status_code}")
        return 0

    json_response = response.json()
    data = json_response.get('results', [])

    if not data:
        print("[INFO] No data returned.")
        return 0
    
    print("[DEBUG] Sample entry:\n", json.dumps(data[0], indent=2))

    # Collect all possible keys
    all_keys = set()
    for item in data:
        all_keys.update(item.keys())

    headers = list(all_keys)

    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    # Write to CSV
    with open(output_path, "w", newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for item in data:
            complete_item = {key: item.get(key, None) for key in headers}
            writer.writerow(complete_item)

    print(f"[SUCCESS] Saved {len(data)} rows to {output_path}")
    return len(data)

if __name__ == "__main__":
    fetch_fda_data()
