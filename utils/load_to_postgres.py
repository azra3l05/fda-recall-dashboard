import psycopg2
import pandas as pd
from datetime import datetime

# DB connection settings
DB_CONFIG = {
    'host': 'localhost',        
    'port': 5433,
    'dbname': 'fda_recall_db',
    'user': 'azra3l',
    'password': 'Re@p3r1802@1002'
}

TABLE_NAME = 'recalls'
CSV_PATH = 'data/fda_recall_2023_cleaned.csv'

def load_data():
    # Load CSV
    df = pd.read_csv(CSV_PATH)

    # Convert NaN to None
    df = df.where(pd.notnull(df), None)

    # Connect to Postgres
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
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
            INSERT INTO {TABLE_NAME} (
                recall_number, product_type, recalling_firm, classification,
                city, state, country, reason_for_recall, status, product_description,
                code_info, distribution_pattern, report_date, recall_initiation_date,
                event_id, termination_date
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, values)

    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ Loaded {len(df)} rows into {TABLE_NAME}")

def parse_date(date_value):
    if pd.isnull(date_value):
        return None
    try:
        return pd.to_datetime(date_value).date()
    except:
        return None

if __name__ == "__main__":
    load_data()
