import psycopg2
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import time

# Setup logging with weekly rotation
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

log_handler = TimedRotatingFileHandler(
    filename=os.path.join(LOG_DIR, "load_to_postgres.log"),
    when='W0',           # Rotate every Monday
    interval=1,
    backupCount=4,       # Keep logs for 4 weeks
)
log_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

logging.basicConfig(
    level=logging.INFO,
    handlers=[log_handler, logging.StreamHandler()]
)

# DB connection settings
DB_CONFIG = {
    'host': 'postgres',        
    'port': 5432,
    'dbname': 'fda_recall_db',
    'user': 'azra3l',
    'password': 'Re@p3r1802@1002'
}

SCHEMA_NAME = 'fda'
TABLE_NAME = 'recalls'

def load_data():
    start_time = time.time()
    # Determine output path with today's date
    date_str = datetime.now().strftime("%Y%m%d")
    CSV_PATH = f'data/cleaned/fda_recall_cleaned_{date_str}.csv'

    if not os.path.exists(CSV_PATH):
        logging.error(f"File not found: {CSV_PATH}")
        return 0

    logging.info(f"Reading data from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    df = df.where(pd.notnull(df), None)
    
    try:
        # Connect to Postgres
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        logging.info("Connected to Postgres database.")

        
        cur.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                recall_number TEXT,
                product_type TEXT,
                recalling_firm TEXT,
                classification TEXT,
                city TEXT,
                state TEXT,
                country TEXT,
                reason_for_recall TEXT,
                status TEXT,
                product_description TEXT,
                code_info TEXT,
                distribution_pattern TEXT,
                report_date DATE,
                recall_initiation_date DATE,
                event_id TEXT,
                termination_date DATE
            );
        """)
        conn.commit()

        logging.info(f"Ensured table `{TABLE_NAME}` exists.")

        # Insert rows
        for _, row in df.iterrows():
            values = [
                row.get("recall_number"),
                row.get("product_type"),
                row.get("recalling_firm"),
                row.get("classification"),
                row.get("city"),
                row.get("state"),
                row.get("country"),
                row.get("reason_for_recall"),
                row.get("status"),
                row.get("product_description"),
                row.get("code_info"),
                row.get("distribution_pattern"),
                parse_date(row.get("report_date")),
                parse_date(row.get("recall_initiation_date")),
                row.get("event_id"),
                parse_date(row.get("termination_date")),
            ]
            cur.execute(f"""
                INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                    recall_number, product_type, recalling_firm, classification,
                    city, state, country, reason_for_recall, status, product_description,
                    code_info, distribution_pattern, report_date, recall_initiation_date,
                    event_id, termination_date
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, values)

        conn.commit()
        logging.info(f"✅ Loaded {len(df)} rows into `{TABLE_NAME}`")

        # Clean up CSV
        os.remove(CSV_PATH)
        logging.info(f"🧹 Deleted source file: {CSV_PATH}")

    except Exception as e:
        logging.exception("❌ Error during database operation")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Closed Postgres connection.")

        duration = time.time() - start_time
        logging.info(f"⏱️ Execution duration: {duration:.2f} seconds")

def parse_date(date_value):
    if pd.isnull(date_value):
        return None
    try:
        return pd.to_datetime(date_value).date()
    except:
        return None

if __name__ == "__main__":
    load_data()
