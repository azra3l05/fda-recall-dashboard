import requests
import pandas as pd
from datetime import datetime
import os
import time
import logging
from logging.handlers import TimedRotatingFileHandler


# Logging setup
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "fetch_fda_data.log"),
    when='W0',  # Rotate every Monday
    interval=1,
    backupCount=4
)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()]
)

# Output directories
RAW_DIR = "data/raw"
CLEANED_DIR = "data/cleaned"
os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(CLEANED_DIR, exist_ok=True)

API_URL = "https://api.fda.gov/food/enforcement.json"
LIMIT = 1000  # adjust if needed

def fetch_fda_data():
    start_time = time.time()
    logging.info("🚀 Starting FDA data fetch")

    all_results = []
    skip = 0

    while True:
        logging.info(f"📦 Fetching records {skip} to {skip + LIMIT}...")
        params = {"limit": LIMIT, "skip": skip}
        try:
            response = requests.get(API_URL, params=params)
            response.raise_for_status()
            data = response.json()

            results = data.get("results", [])
            if not results:
                break

            all_results.extend(results)
            skip += LIMIT
        except Exception as e:
            logging.exception("❌ Failed to fetch data from FDA API")
            break

    if not all_results:
        logging.warning("⚠️ No data fetched. Exiting.")
        return

    # Save raw JSON
    date_str = datetime.now().strftime("%Y%m%d")
    raw_path = os.path.join(RAW_DIR, f"fda_recall_raw_{date_str}.json")
    with open(raw_path, "w") as f:
        pd.DataFrame(all_results).to_json(f, orient="records", lines=True)
    logging.info(f"💾 Raw data saved to {raw_path}")

    # Clean and save CSV
    df = pd.DataFrame(all_results)
    cleaned_path = os.path.join(CLEANED_DIR, f"fda_recall_cleaned_{date_str}.csv")
    df.to_csv(cleaned_path, index=False)
    logging.info(f"🧼 Cleaned data saved to {cleaned_path} with {len(df)} rows")

    # Cleanup raw file
    try:
        os.remove(raw_path)
        logging.info(f"🧹 Deleted raw file: {raw_path}")
    except Exception as e:
        logging.warning(f"⚠️ Could not delete raw file: {raw_path}")

    duration = time.time() - start_time
    logging.info(f"✅ Fetch and clean complete in {duration:.2f} seconds")

if __name__ == "__main__":
    fetch_fda_data()