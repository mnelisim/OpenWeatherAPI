from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from OpenWeatherAPI.storage.db import create_connection
from OpenWeatherAPI.main import extract_data, transform_data, load_data, generate_report
from OpenWeatherAPI.logs.logging import log_message

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'weather_etl',
    default_args=default_args,
    description='ETL pipeline for weather data',
    schedule_interval='@hourly',
    start_date=datetime(2026, 3, 16),
    catchup=False
)

# Creating connection for tasks
def get_connection():
    conn, cur = create_connection(datetime.now().strftime("%Y%m%d_%H%M%S"))
    return conn, cur

def extract_task(**kwargs):
    conn, cur = get_connection()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    data_raw = extract_data(cur, conn, ts)
    kwargs['ti'].xcom_push(key='data_raw', value=data_raw)
    cur.close()
    conn.close()

def transform_task(**kwargs):
    conn, cur = get_connection()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    data_raw = kwargs['ti'].xcom_pull(key='data_raw', task_ids='extract_task')
    data_clean = transform_data(cur, conn, data_raw, ts)
    kwargs['ti'].xcom_push(key='data_clean', value=data_clean)
    cur.close()
    conn.close()

def load_task(**kwargs):
    conn, cur = get_connection()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    data_clean = kwargs['ti'].xcom_pull(key='data_clean', task_ids='transform_task')
    load_data(cur, conn, data_clean, ts)
    cur.close()
    conn.close()

def report_task(**kwargs):
    conn, cur = get_connection()
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    data_clean = kwargs['ti'].xcom_pull(key='data_clean', task_ids='transform_task')
    generate_report(cur, conn, data_clean, ts)
    cur.close()
    conn.close()

# Define tasks
Extract = PythonOperator(task_id='extract_task', python_callable=extract_task, dag=dag)
Transform = PythonOperator(task_id='transform_task', python_callable=transform_task, dag=dag)
Load = PythonOperator(task_id='load_task', python_callable=load_task, dag=dag)
Report = PythonOperator(task_id='report_task', python_callable=report_task, dag=dag)

# Set dependencies
Extract >> Transform >> Load >> Report