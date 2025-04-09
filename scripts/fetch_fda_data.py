import requests
import pandas as pd
from datetime import datetime, timedelta
import os
import sys
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from urllib.parse import quote_plus

# Include notifier path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from notifier import send_discord_notification

# === Logging setup ===
def setup_logger(log_file_name: str, logger_name="fetch_fda_logger"):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        log_dir = "/opt/airflow/logs"
        os.makedirs(log_dir, exist_ok=True)

        file_handler = TimedRotatingFileHandler(
            filename=os.path.join(log_dir, log_file_name),
            when="midnight", interval=1, backupCount=7
        )
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        logger.addHandler(file_handler)

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter("[%(levelname)s] %(message)s"))
        logger.addHandler(console_handler)

    return logger

logger = setup_logger("fetch_fda_data.log")

# Output directories
RAW_DIR = "/opt/airflow/data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

API_URL = "https://api.fda.gov/drug/enforcement.json"
LIMIT = 1000  # OpenFDA max limit per request

def generate_month_ranges(start_year, start_month, end_year, end_month):
    current = datetime(start_year, start_month, 1)
    end = datetime(end_year, end_month, 1)
    while current <= end:
        next_month = (current.replace(day=28) + timedelta(days=4)).replace(day=1)
        yield current.strftime("%Y%m%d"), (next_month - timedelta(days=1)).strftime("%Y%m%d")
        current = next_month

def fetch_fda_data():
    send_discord_notification("🚀 FDA data fetch job started!")
    start_time = time.time()
    logging.info("🚀 Starting FDA historical data fetch")
    all_data = []

    for from_date, to_date in generate_month_ranges(2020, 1, 2025, 3):
        logging.info(f"📅 Fetching data from {from_date} to {to_date}")
        skip = 0

        while True:
            search_query = f"report_date:[{from_date} TO {to_date}]"
            encoded_query = quote_plus(search_query, safe=":[]")  # Keep :, [, ]
            url = f"{API_URL}?search={encoded_query}&limit={LIMIT}&skip={skip}"

            try:
                response = requests.get(url)
                if response.status_code == 400:
                    logging.warning(f"📭 No more data for {from_date} to {to_date} at skip {skip}")
                    break
                response.raise_for_status()
                results = response.json().get("results", [])

                if not results:
                    logging.info(f"📭 No results for {from_date} to {to_date} at skip {skip}")
                    break

                all_data.extend(results)
                logging.info(f"📦 Fetched {len(results)} records from {from_date} to {to_date}, skip {skip}")
                skip += LIMIT
            except requests.exceptions.HTTPError as http_err:
                # send_discord_notification(f"❌ HTTP error {response.status_code} for {from_date}-{to_date}: {http_err}")
                logging.exception("❌ HTTP error")
                break
            except Exception as e:
                send_discord_notification(f"❌ Failed to fetch {from_date}-{to_date}: {e}")
                logging.exception("❌ General error")
                break

    if not all_data:
        send_discord_notification("⚠️ No data fetched from FDA")
        logging.warning("⚠️ No data fetched.")
        return

    # Save output
    date_str = datetime.now().strftime("%Y%m%d")
    raw_path = os.path.join(RAW_DIR, f"fda_recall_raw_{date_str}.json")
    df = pd.DataFrame(all_data)
    df.to_json(raw_path, orient="records", lines=True)
    df.to_csv(raw_path.replace(".json", ".csv"), index=False)

    send_discord_notification(f"✅ Fetched {len(df)} records from {from_date} to {to_date} and saved to CSV.")
    logging.info(f"✅ Fetched {len(df)} total records and saved to {raw_path}")

    duration = time.time() - start_time
    logging.info(f"⏱️ Completed in {duration:.2f} seconds.")
    send_discord_notification(f"⏱️ Execution duration: {duration:.2f} seconds")

if __name__ == "__main__":
    fetch_fda_data()
