-- models/weather.sql
WITH raw_data AS (
    SELECT *
    FROM public.weatherdata
)

SELECT
    city,
    date_trunc('hour', timestamp) AS hour,
    AVG(temperature) AS avg_temperature,
    AVG(humidity) AS avg_humidity
FROM raw_data
GROUP BY city, hour