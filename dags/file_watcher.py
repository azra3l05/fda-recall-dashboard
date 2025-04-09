import os
import time
from datetime import datetime
import logging


def wait_for_csv_file(timeout=120, interval=5, prefix="fda_recall_raw_", data_dir="/opt/airflow/data/raw"):
    """
    Waits for a CSV file with today's date in the filename to be created in the specified directory.

    Args:
        timeout (int): Maximum wait time in seconds (default: 120).
        interval (int): Time between checks in seconds (default: 5).
        prefix (str): Filename prefix before the date (default: 'fda_recall_raw_').
        data_dir (str): Directory path to monitor (default: '/opt/airflow/data/raw').

    Returns:
        str: Full path of the detected CSV file.

    Raises:
        FileNotFoundError: If the file does not appear within the timeout period.
    """
    date_str = datetime.now().strftime("%Y%m%d")
    expected_file = os.path.join(data_dir, f"{prefix}{date_str}.csv")
    logging.info(f"⏳ Waiting for file: {expected_file}")

    waited = 0
    while waited < timeout:
        if os.path.exists(expected_file):
            logging.info(f"✅ File found: {expected_file}")
            return expected_file
        time.sleep(interval)
        waited += interval
        logging.debug(f"⏲️ Still waiting... ({waited}/{timeout} seconds)")

    error_msg = f"❌ File not found within timeout: {expected_file}"
    logging.error(error_msg)
    raise FileNotFoundError(error_msg)
