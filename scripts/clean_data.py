import pandas as pd
from pathlib import Path
from datetime import datetime

def clean_fda_data(input_path=None, output_path=None, **kwargs):
    # Generate current date string if not passed via Airflow
    date_str = datetime.now().strftime("%Y%m%d")

    if input_path is None:
        input_path = f"data/raw/fda_recall_{date_str}.csv"

    if output_path is None:
        output_path = f"data/cleaned/fda_recall_cleaned_{date_str}.csv"

    input_path = Path(input_path)
    output_path = Path(output_path)

    if not input_path.exists():
        print(f"[ERROR] File not found: {input_path}")
        return 0

    # Load the CSV
    print(f"[INFO] Reading file: {input_path}")
    df = pd.read_csv(input_path)
    print("[DEBUG] Columns in input:", df.columns.tolist())

    # Convert date columns to datetime
    date_columns = ["report_date", "recall_initiation_date", "event_date_started", "event_date_ended", "termination_date"]
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(
                df[col].dropna().apply(lambda x: str(int(x)) if pd.notna(x) else x),
                format="%Y%m%d",
                errors="coerce"
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
    df.drop_duplicates(inplace=True)

    # Drop rows missing required fields
    required_columns = ["recall_number", "product_type"]
    df.dropna(subset=required_columns, inplace=True)

    # Save the cleaned file
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    print(f"[SUCCESS] Cleaned data saved to {output_path}")
    print(f"🔢 Rows after cleaning: {len(df)}")
    
    # Push to XCom if inside Airflow
    if 'ti' in kwargs:
        kwargs['ti'].xcom_push(key='cleaned_data_path', value=str(output_path))

    return len(df)

# ✅ Call the function
if __name__ == "__main__":
    clean_fda_data()