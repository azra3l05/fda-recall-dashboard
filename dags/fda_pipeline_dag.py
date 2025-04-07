from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import json
from scripts.fetch_fda_data import fetch_fda_data
from scripts.clean_data import clean_fda_data
from scripts.load_to_postgres import load_data

default_args = {
    'owner': 'azra3l',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='fda_recall_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['fda', 'recalls', 'postgres']
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
        task_id='load_to_postgres',
        python_callable=load_data
    )

    fetch_data >> clean_data >> load_data
