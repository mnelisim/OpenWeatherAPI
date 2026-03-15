# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# # from ingestion.data_fetching import fetch_weather_data
# # from processing.cleaning import clean_weather
# # from storage.db import create_connection
# # from storage.load_to_db import create_table_if_not_exists, store_data_in_db
# from main import run_etl
# import sys
# import os

# sys.path.append(os.path.abspath("/opt/airflow/OpenWeatherAPI"))


# default_args = {
#     "owner": "mnelisi",
#     "start_date": datetime(2024, 1, 1),
# }

# dag = DAG(
#     dag_id="weather_pipeline",
#     default_args=default_args,
#     schedule_interval="@daily",
#     catchup=False
# )

# def extract():
#     print("Extracting weather data")


# def transform():
#     print("Transforming data")


# def load():
#     print("Loading into database")

# extract_task = PythonOperator(
#     task_id="extract_weather",
#     python_callable=extract,
#     dag=dag
# )

# transform_task = PythonOperator(
#     task_id="transform_weather",
#     python_callable=transform,
#     dag=dag
# )

# load_task = PythonOperator(
#     task_id="load_weather",
#     python_callable=load,
#     dag=dag
# )

# #extract_task >> transform_task >> load_task
# run_etl()

# /opt/airflow/dags/weather_dags.py
import sys
import os

# Add your project folder to the path
sys.path.append(os.path.abspath("/opt/airflow/OpenWeatherAPI"))

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Now this import will work
from main import run_etl

default_args = {
    "owner": "mnelisi",
    "start_date": datetime(2024, 1, 1),
}

dag = DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

etl_task = PythonOperator(
    task_id="run_full_etl",
    python_callable=run_etl,
    dag=dag
)