from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
from scripts.fetch_fda_data import fetch_fda_data
from scripts.clean_data import clean_fda_data
from utils.snowflake_loader import load_to_snowflake

default_args = {
    'owner': 'you',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def get_snowflake_credentials(secret_name: str):
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_name)
    secret = json.loads(response['SecretString'])
    return secret

with DAG(
    dag_id='fda_recall_dag',
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['fda', 'recalls', 'snowflake']
) as dag:

    fetch_data = PythonOperator(
        task_id='fetch_fda_data',
        python_callable=fetch_fda_data
    )

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_fda_data
    )

    load_data = PythonOperator(
        task_id='load_to_snowflake',
        python_callable=lambda: load_to_snowflake(
            get_snowflake_credentials("fda_snowflake_secret")
        )
    )

    fetch_data >> clean_data >> load_data
