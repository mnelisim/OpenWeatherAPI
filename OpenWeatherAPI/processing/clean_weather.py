import csv
import pandas as pd
import os
from datetime import datetime
from logs.logging import log_message

def parse_json(all_data, timestamp_file):
    rows = []
    for data in all_data:
        rows.append({
            "city": data['name'],
            "weather": data['weather'][0]['description'].title(), # Capitalise words
            "windspeed": round(data['wind']['speed'] * 3.6, 2), # km/h
            "humidity": data['main']['humidity'],
            "temperature": round(data['main']['temp'], 2), # Celsius
            "longitude":round(data['coord']['lon'], 4),
            'latitude': round(data['coord']['lat'], 4),
            "timestamp":datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })

# Create DataFrame 
    df = pd.DataFrame(rows)
    
    os.makedirs(f"reports/csv", exist_ok=True)
    csv_filename = f"reports/csv/weather_{timestamp_file}.csv"
    df.to_csv(csv_filename, index=False)
    log_message(f"CSV file created: {csv_filename}", run_id=timestamp_file)

    return df
