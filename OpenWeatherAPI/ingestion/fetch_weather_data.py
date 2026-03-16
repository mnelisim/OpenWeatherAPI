import requests
import time
import json
import os
from datetime import datetime
from OpenWeatherAPI.config import WEATHER_API_KEY, BASE_URL
from OpenWeatherAPI.logs.logging import log_message

#Extract api
def main_request(retries=3, timestamp_file=None):
    cities = ["johannesburg", "cape town", "durban", "pretoria"]
    all_weather_data = []

    for city in cities:
        success = True # flag to show that the city data is fetched
        api_url = f"{BASE_URL}?q={city}&appid={WEATHER_API_KEY}&units=metric"

        for attempt in range(retries):
            try:
                log_message(f"Fetching weather for: {city} (Attempt {attempt + 1})", run_id=timestamp_file)
                response = requests.get(api_url, timeout=10)
                response.raise_for_status()

                #Append json to list
                all_weather_data.append(response.json())
                log_message(f"API Request Successful {city}", run_id=timestamp_file)
                success = True
                break # break the try loop and go fetch the city data
            
            except requests.exceptions.HTTPError as HTTP_err:
                log_message(f"HTTP error for {city}: {HTTP_err} (City may be wrong or API key invalid)", run_id=timestamp_file)
                break # No point retrying for a 401 404 error

            except requests.exceptions.ConnectionError as HTTP_err:
                log_message(f"Connection error for {city}. Retry in 3 seconds...", run_id=timestamp_file)
                time.sleep(3)
                
            except requests.exceptions.Timeout:
                log_message(f"Timeout error for {city}. Retry...", run_id=timestamp_file)
                time.sleep(2)

            except Exception as err:
                log_message(f"Unexpected error for {city}: {err}", run_id=timestamp_file)
                break
        if not success:
           log_message(f"Failed to fetch data for {city} after retry.", run_id=timestamp_file)

    return all_weather_data if all_weather_data else None

def save_raw_json(data, timestamp_file):
    #timestamp_file = datetime.now().strftime("%Y%m%d_%H%M%S")

    os.makedirs(f"data/raw", exist_ok=True)
    raw_filename = f"data/raw/weather_report_{timestamp_file}.json"
    with open(raw_filename, "w") as f:
        json.dump(data, f, indent=4)

    log_message(f"Raw data saved Generated: {raw_filename}", run_id=timestamp_file)


    