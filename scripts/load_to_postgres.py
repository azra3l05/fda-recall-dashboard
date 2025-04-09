import psycopg2
import pandas as pd
from datetime import datetime
import logging
from logging.handlers import TimedRotatingFileHandler
import os
import sys
import time

# Include notifier path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from notifier import send_discord_notification


# === Logging setup ===
def setup_logger(log_file_name: str, logger_name="loadtopostgres_logger"):
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

logger = setup_logger("load_postgres_data.log")

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
    send_discord_notification("🚀 Data Load to Postgres job started!")
    start_time = time.time()
    # Determine output path with today's date
    date_str = datetime.now().strftime("%Y%m%d")
    CSV_PATH = f'/opt/airflow/data/cleaned/fda_recall_cleaned_{date_str}.csv'

    if not os.path.exists(CSV_PATH):
        logging.error(f"File not found: {CSV_PATH}")
        send_discord_notification(f"❌ File not found: {CSV_PATH}")
        return 0

    logging.info(f"Reading data from: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)
    df = df.where(pd.notnull(df), None)
    
    try:
        # Connect to Postgres
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        send_discord_notification("🔗 Connected to Postgres database.")
        logging.info("Connected to Postgres database.")

        # Ensure schema and table exist with a UNIQUE constraint on recall_number
        cur.execute(f"""
            CREATE SCHEMA IF NOT EXISTS {SCHEMA_NAME};
            CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.{TABLE_NAME} (
                recall_number TEXT UNIQUE,
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
                termination_date DATE,
                openfda TEXT,
                address_1 TEXT,
                address_2 TEXT,
                postal_code TEXT,
                voluntary_mandated TEXT,
                initial_firm_notification TEXT,
                product_quantity TEXT,
                center_classification_date DATE,
                more_code_info TEXT
            );
        """)
        conn.commit()

        logging.info(f"Ensured table `{TABLE_NAME}` exists with unique recall_number.")

        inserted = 0
        skipped = 0

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
                row.get("report_date"),
                row.get("recall_initiation_date"),
                row.get("event_id"),
                row.get("termination_date"),
                row.get("openfda"),
                row.get("address_1"),
                row.get("address_2"),
                row.get("postal_code"),
                row.get("voluntary_mandated"),
                row.get("initial_firm_notification"),
                row.get("product_quantity"),
                row.get("center_classification_date"),
                row.get("more_code_info")
            ]
            try:
                cur.execute(f"""
                    INSERT INTO {SCHEMA_NAME}.{TABLE_NAME} (
                        recall_number, product_type, recalling_firm, classification,
                        city, state, country, reason_for_recall, status, product_description,
                        code_info, distribution_pattern, report_date, recall_initiation_date,
                        event_id, termination_date, openfda, address_1, address_2,
                        postal_code, voluntary_mandated, initial_firm_notification,
                        product_quantity, center_classification_date, more_code_info
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (recall_number) DO NOTHING
                """, values)
                if cur.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1
            except Exception as row_e:
                logger.warning(f"⚠️ Failed to insert row with recall_number {row.get('recall_number')}: {row_e}")
                conn.rollback()  # Reset transaction state so it can continue

        conn.commit()
        logger.info(f"✅ Inserted {inserted} new rows into `{TABLE_NAME}`")
        send_discord_notification(f"✅ Inserted {inserted} new rows into `{TABLE_NAME}`")
        logger.info(f"⏭️ Skipped {skipped} duplicate rows")
        send_discord_notification(f"⏭️ Skipped {skipped} duplicate rows")

        # # Clean up CSV
        # os.remove(CSV_PATH)
        # logging.info(f"🧹 Deleted source file: {CSV_PATH}")

    except Exception as e:
        logging.exception("❌ Error during database operation")
        send_discord_notification(f"❌ Error during database operation: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()
        logging.info("Closed Postgres connection.")

        duration = time.time() - start_time
        logging.info(f"⏱️ Execution duration: {duration:.2f} seconds")
        send_discord_notification(f"⏱️ Execution duration: {duration:.2f} seconds")

def parse_date(date_value):
    if pd.isnull(date_value):
        return None
    try:
        return pd.to_datetime(date_value).date()
    except:
        return None

if __name__ == "__main__":
    load_data()
