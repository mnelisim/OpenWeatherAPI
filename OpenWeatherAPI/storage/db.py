import psycopg2
import time
from config import DB_NAME,DB_HOST,DB_USER,DB_PASSWORD,DB_PORT
from logs.logging import log_message


#Database connection
def create_connection(timestamp_file):
    while True:
        try:
            conn = psycopg2.connect(
                database=DB_NAME,host=DB_HOST,user=DB_USER,password=DB_PASSWORD,port=DB_PORT) 
            cur = conn.cursor()
            
            log_message("Database connection successfully!", run_id=timestamp_file)
            return conn, cur
        
        except Exception as error:
            log_message("Connection to database failed", run_id=timestamp_file)
            log_message(f"Error: {error}", run_id=timestamp_file)
            time.sleep(2) #Wait 2 seconds and retry
            