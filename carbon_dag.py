from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from carbon_etl import get_carbon_data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'carbon_dag',
    default_args=default_args,
    description='Carbon Details DAG',
    schedule_interval=timedelta(days=1),
)

run_etl = PythonOperator(
    task_id='complet_carbon_data_etl',
    python_callable=get_carbon_data,
    dag=dag, 
)

run_etl
