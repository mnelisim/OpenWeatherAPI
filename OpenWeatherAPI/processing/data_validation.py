from OpenWeatherAPI.logs.logging import log_message


def validate_weather_data(data, run_id):
    """Validate raw and cleaned data
    Return true if valid or otherwise false"""
    
    all_errors = []
    
    for idx, row in data.iterrows():  
        # Validate Wind Speed
        if not (0 <= row["windspeed"] <= 75):
            all_errors.append(f"Row {idx} ({row['city']}): Invalid wind speed {row['windspeed']}")

        # Validate Temperature (Check if it's a number and within sane Earth bounds)
        if not isinstance(row["temperature"], (int, float, complex)) or not (-60 <= row["temperature"] <= 60):
            all_errors.append(f"Row {idx} ({row['city']}): Invalid temperature {row['temperature']}")

        # Validate Humidity
        if not (0 <= row["humidity"] <= 100):
            all_errors.append(f"Row {idx} ({row['city']}): Invalid humidity {row['humidity']}")

        # Validate Coordinates
        if not (-180 <= row["longitude"] <= 180):
            all_errors.append(f"Row {idx} ({row['city']}): Invalid longitude {row['longitude']}")
        if not (-90 <= row["latitude"] <= 90):
            all_errors.append(f"Row {idx} ({row['city']}): Invalid latitude {row['latitude']}")

    # --- After the loop is completely finished, check if the is any errors ---
    if all_errors:
        log_message(f"Data validation failed: {len(all_errors)} issues found", run_id=run_id)
        for e in all_errors:
            log_message(e, run_id=run_id)
        return False  # Failed validation
    
    log_message("Data validation passed for all rows", run_id=run_id)
    return True  # Passed validation


        