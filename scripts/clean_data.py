import pandas as pd
from pathlib import Path
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import os
import sys

# Include notifier path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from notifier import send_discord_notification
from file_watcher import wait_for_csv_file


# === Logging setup ===
def setup_logger(log_file_name: str, logger_name="clean_fda_logger"):
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

logger = setup_logger("clean_fda_data.log")

def clean_fda_data():
    send_discord_notification("🚀 Data cleaning job started!")
    start_time = time.time()
    date_str = datetime.now().strftime("%Y%m%d")

    # Wait for the CSV file to be available
    try:
        input_file = wait_for_csv_file(timeout=180, interval=10, prefix="fda_recall_raw_")
    except FileNotFoundError as e:
        send_discord_notification(f"❌ {str(e)}")
        return 0

    input_path = Path(input_file)
    output_path = Path(f"/opt/airflow/data/cleaned/fda_recall_cleaned_{date_str}.csv")
    output_path.parent.mkdir(parents=True, exist_ok=True)

    logging.info(f"📖 Reading file: {input_path}")
    df = pd.read_csv(input_path)
    logging.debug(f"🔍 Columns in input: {df.columns.tolist()}")

    # Convert date columns to datetime
    date_columns = ["report_date", "recall_initiation_date", "event_date_started", "event_date_ended", "termination_date","center_classification_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col].dropna().apply(lambda x: str(int(x)) if pd.notna(x) else x),
                format="%Y%m%d", errors="coerce"
            )

    # Fill missing values
    fill_values = {
        "city": "Unknown",
        "state": "Unknown",
        "product_description": "Not Provided",
        "reason_for_recall": "Not Specified",
    }
    df.fillna(value=fill_values, inplace=True)

    # Remove duplicates
    before = len(df)
    df.drop_duplicates(inplace=True)
    logging.info(f"🧹 Removed {before - len(df)} duplicate rows")

    # Drop rows missing required fields
    required_columns = ["recall_number", "product_type"]
    df.dropna(subset=required_columns, inplace=True)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    send_discord_notification(f"✅ Cleaned data saved to {output_path} and Final row count: {len(df)}")
    logging.info(f"✅ Cleaned data saved to {output_path}")
    logging.info(f"📊 Final row count: {len(df)}")

    # Cleanup input file
    # try:
    #     input_path.unlink()
    #     logging.info(f"🧽 Deleted raw input file: {input_path}")
    # except Exception as e:
    #     logging.warning(f"⚠️ Could not delete input file: {input_path}. Reason: {e}")

    duration = time.time() - start_time
    logging.info(f"⏱️ Cleaning completed in {duration:.2f} seconds")
    send_discord_notification(f"⏱️ Execution duration: {duration:.2f} seconds")
    return len(df)

if __name__ == "__main__":
    clean_fda_data()
