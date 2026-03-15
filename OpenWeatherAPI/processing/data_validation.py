from logs.logging import log_message


def validate_weather_data(data, run_id):
    """Validate raw and cleaned data
    Return true if valid or otherwise false"""

    errors = []
    
    row = data.iloc[0]

    if  not (0 <= row["windspeed"] <= 75):
        errors.append(f"Invalid wind speed")

    if  not isinstance(row["temperature"], (int, float)):
        errors.append(f"Invalid Temperature: {row}")

    if not (0 <= row["humidity"] <= 100):
        errors.append(f"Invalid Humidity: {row}")

    if not (-180 <= row["longitude"] <= 180):
        errors.append(f"Invalid Longitude: {row}")

    if not (-90 <= row["latitude"] <= 90):
        errors.append(f"Invalid Latitude: {row}")

    if errors:
        log_message(f"Data validation failed: {len(errors)}, errors")
        for e in errors:
            log_message(e, run_id=run_id)
        return False
    else:
        log_message(f"Data validation passed", run_id=run_id)
        return True
            

    