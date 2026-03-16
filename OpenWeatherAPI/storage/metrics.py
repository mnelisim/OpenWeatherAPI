import psycopg2
import time
from datetime import datetime
from OpenWeatherAPI.config import table_name,DB_NAME,DB_HOST,DB_USER,DB_PASSWORD,DB_PORT
from OpenWeatherAPI.logs.logging import log_message

#===================create_table_if_not_exists=========
def create_metrics_table(cur, conn, run_id=None):
    try:
        cur.execute(f"""
                CREATE TABLE IF NOT EXISTS pipeline_metrics (
                    id SERIAL PRIMARY KEY,
                    run_id TEXT,
                    task_name TEXT,
                    rows_processed INT,
                    status TEXT,
                    start_time TIMESTAMP,
                    end_time TIMESTAMP,
                    runtime_seconds FLOAT,
                    error_message TEXT)
                """)
                
        conn.commit()
        if run_id:
          log_message(f"Table metrics is ready", run_id=run_id)
    except Exception as e:
        conn.rollback()
        log_message(f"Failed to create table metrics: {e}")

#====================LOAD DATA INTO DATABASE===================
def log_metrics(cur, conn,
                run_id, task_name, rows, status, start, end, error=None):
    try:

        #Calculate runtime records
        runtime_seconds = (end - start).total_seconds() if start and end else None

        query = f"""
        INSERT INTO pipeline_metrics (
            run_id,
            task_name,
            rows_processed,
            status,
            start_time,
            end_time,
            runtime_seconds,
            error_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """
        values =(
            run_id,
            task_name,
            rows,
            status,
            start,
            end,
            runtime_seconds,
            error
        )
        
        cur.execute(query, values)
        #push data into db
        conn.commit()
        log_message(f"Data for metrics successfully inserted pipeline_metrics")
    except Exception as error:
       conn.rollback()
       log_message(f"Metrics logging failed: {error}")
