import psycopg2
import time
import pandas as pd
from datetime import datetime
from config import table_name,DB_NAME,DB_HOST,DB_USER,DB_PASSWORD,DB_PORT
from logs.logging import log_message

#===================create_table_if_not_exists=========
def create_table_if_not_exists(cur, conn, table_name, timestamp_file):
    try:
        cur.execute(f"""
             CREATE TABLE IF NOT EXISTS {table_name} (
                    id SERIAL PRIMARY KEY,
                    city VARCHAR(100),
                    description VARCHAR(255),
                    windspeed NUMERIC(5,2),
                    humidity INTEGER,
                    temperature NUMERIC(5,2),
                    longitude NUMERIC(8,5),
                    latitude NUMERIC(8,5),
                    timestamp TIMESTAMP);
        """)
        conn.commit()
        log_message(f"Table '{table_name}' is ready", run_id=timestamp_file)
    except Exception as e:
        conn.rollback()
        log_message(f"Failed to create the table: {e}", run_id=timestamp_file)

#====================LOAD DATA INTO DATABASE===================
def store_data_in_db(cur, conn, data, table_name, timestamp_file):
    try:
        for _, row in data.iterrows():
            ts = pd.to_datetime(row['timestamp']).to_pydatetime()

            query = f"""INSERT INTO {table_name} (city,description,windspeed,humidity,temperature,longitude,latitude,timestamp)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""" 

            #Ensure all variables are native Python types
            values = (
                str(row['city']),
                str(row['weather']),
                float(row['windspeed']),
                float(row['humidity']),
                float(row['temperature']),
                float(row['longitude']),
                float(row['latitude']),
                ts)
            
            cur.execute(query, values)
            #push data into db
            conn.commit()
            log_message(f"Data for {row['city']} successfully inserted to: {table_name}", run_id=timestamp_file)

    except Exception as error:
       conn.rollback()
       log_message(f"Data not inserted in DB: {error}", run_id=timestamp_file)
       raise
