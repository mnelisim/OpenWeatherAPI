# config.py
from dotenv import load_dotenv
import os

load_dotenv()  # loads variables from .env

# ===================== CONFIG API ====================
WEATHER_API_KEY = os.getenv("WEATHER_API_KEY")
BASE_URL = "https://api.openweathermap.org/data/2.5/weather"

# ===================== CONFIG TABLE ====================
table_name = "weatherdata"
table_name2 = "pipeline_metrics"

# ===================== CONFIG DATABASE ====================
DB_NAME = os.getenv("DB_NAME")
DB_HOST = os.getenv("DB_HOST")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

