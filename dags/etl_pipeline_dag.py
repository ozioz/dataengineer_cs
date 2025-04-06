from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.etl_pipeline import etl_process

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
}

with DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False
) as dag:
    
    etl_task = PythonOperator(
        task_id='run_etl',
        python_callable=etl_process,
        retries=3
    )