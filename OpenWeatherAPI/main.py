from datetime import datetime
from OpenWeatherAPI.config import table_name
from OpenWeatherAPI.ingestion.fetch_weather_data import main_request, save_raw_json
from OpenWeatherAPI.processing.clean_weather import parse_json
from OpenWeatherAPI.storage.db import create_connection
from OpenWeatherAPI.storage.load_to_db import store_data_in_db, create_table_if_not_exists
from OpenWeatherAPI.storage.metrics import create_metrics_table, log_metrics
from OpenWeatherAPI.reports.generate_reports import create_pdf_report
from OpenWeatherAPI.logs.logging import log_message
from OpenWeatherAPI.processing.data_validation import validate_weather_data

#def run_etl():
#   conn = None
#   cur = None

     # Generate timestamp the entire ETL run
#    timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")

#    try:
        #Connect to DB
#        conn, cur = create_connection(timestamp_file)
#        create_metrics_table(cur, conn, run_id=timestamp_file)
def extract_data(cur, conn, timestamp_file):
        #Extract: fetch weather data from API
        start = datetime.now()
        data_raw = main_request(timestamp_file=timestamp_file)
        end = datetime.now()
        if not data_raw:
            log_message("ETL stopped due to API failure",run_id=timestamp_file)
            log_metrics(cur, conn, timestamp_file, "extract", 0, "failed", start, end, error="API Failed")
            exit(1)
        log_metrics(cur, conn, timestamp_file, "extract", len(data_raw), "success", start, end)
        return data_raw

def transform_data(cur, conn, data_raw, timestamp_file):
        #Save raw data JSON
        start = datetime.now()
        save_raw_json(data_raw, timestamp_file)  
        #Transform clean data and save csv
        data_clean = parse_json(data_raw, timestamp_file)
        end = datetime.now()
        #Data validation
        valid = validate_weather_data(data_clean, timestamp_file)
        if not valid:
            log_metrics(cur, conn, timestamp_file, "Validate", 0, "failed", start, end, error="validation failed")
            exit(1)
        log_metrics(cur, conn, timestamp_file, "Validate", 1, "success", start, end)
        return data_clean
    
def load_data(cur, conn, data_clean, timestamp_file):
        #Load data
        start = datetime.now()
        #Ensure table exist
        create_table_if_not_exists(cur, conn, table_name, timestamp_file)
        #Load data in DB
        store_data_in_db(cur, conn, data_clean, table_name, timestamp_file)
        end = datetime.now()
        log_metrics(cur, conn, timestamp_file, "load", len(data_clean), "success", start, end)
        
def generate_report(cur, conn, data_clean, timestamp_file):        
        #Generate pdf report
        start = datetime.now()
        create_pdf_report(data_clean, timestamp_file)
        end = datetime.now()
        log_metrics(cur, conn, timestamp_file, "report", len(data_clean), "success", start, end)

        log_message("ETL run successfuly completed", run_id=timestamp_file)

#     except Exception as e:
#         log_message(f"Fatal error in ETL: {e}", run_id=timestamp_file)
#         if cur and conn:
#             log_metrics(cur, conn, timestamp_file, "etl", 0, "failed", datetime.now(), datetime.now(), error=str(e))

#     finally:
#         #Close DB connection
#         if cur is not None:
#             cur.close()
#         if conn is not None:
#             conn.close()

#         log_message("Database connection closed", run_id=timestamp_file)

# if __name__ == "__main__":
#     run_etl()
