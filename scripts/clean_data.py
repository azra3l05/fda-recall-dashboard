import pandas as pd
from pathlib import Path
import time
import logging
from logging.handlers import TimedRotatingFileHandler
from datetime import datetime
import os

# Setup logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "clean_data.log"),
    when='W0',  # Weekly rotation on Mondays
    interval=1,
    backupCount=4
)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()]
)

def clean_fda_data(input_path="data/raw/fda_recall_{date_str}.csv", output_path="data/cleaned/fda_recall_cleaned_{date_str}.csv"):
    start_time = time.time()
    date_str = datetime.now().strftime("%Y%m%d")
    input_path = Path(str(input_path).format(date_str=date_str))
    output_path = Path(str(output_path).format(date_str=date_str))

    if not input_path.exists():
        logging.error(f"❌ File not found: {input_path}")
        return 0

    logging.info(f"📖 Reading file: {input_path}")
    df = pd.read_csv(input_path)
    logging.debug(f"🔍 Columns in input: {df.columns.tolist()}")

    # Convert date columns to datetime
    date_columns = ["report_date", "recall_initiation_date", "event_date_started", "event_date_ended", "termination_date"]
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
    logging.info(f"✅ Cleaned data saved to {output_path}")
    logging.info(f"📊 Final row count: {len(df)}")

    # Cleanup input file
    try:
        input_path.unlink()
        logging.info(f"🧽 Deleted raw input file: {input_path}")
    except Exception as e:
        logging.warning(f"⚠️ Could not delete input file: {input_path}. Reason: {e}")

    duration = time.time() - start_time
    logging.info(f"⏱️ Cleaning completed in {duration:.2f} seconds")
    return len(df)

if __name__ == "__main__":
    clean_fda_data()
