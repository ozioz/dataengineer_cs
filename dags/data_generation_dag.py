from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from scripts.data_generator import generate_shipments

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 2,
}

with DAG(
    'generate_shipments',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:
    
    generate_task = PythonOperator(
        task_id='generate_shipments',
        python_callable=generate_shipments,
        op_kwargs={'num': 50}
    )