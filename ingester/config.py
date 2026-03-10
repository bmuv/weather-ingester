import os
from datetime import date, timedelta

# ----------------------------------------------------------------
# API
# ----------------------------------------------------------------
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"

# Variables to request from Open-Meteo
# Full reference: https://open-meteo.com/en/docs/historical-weather-api
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
    "relative_humidity_2m_mean",
]

# Maps Open-Meteo API variable names → weather_daily DB column names
VARIABLE_MAP = {
    "temperature_2m_max":      "temp_max_c",
    "temperature_2m_min":      "temp_min_c",
    "precipitation_sum":       "precipitation_mm",
    "windspeed_10m_max":       "windspeed_max_kmh",
    "relative_humidity_2m_mean": "humidity_avg_pct",
}

# ----------------------------------------------------------------
# Date range
# ----------------------------------------------------------------
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", 30))

def get_date_range() -> tuple[str, str]:
    """Return (start_date, end_date) as ISO strings for the lookback window."""
    end = date.today() - timedelta(days=1)   # yesterday -- today not yet published
    start = end - timedelta(days=LOOKBACK_DAYS - 1)
    return start.isoformat(), end.isoformat()

# ----------------------------------------------------------------
# Cities
# Must match the seed data in init.sql exactly (name is the join key)
# ----------------------------------------------------------------
CITIES = [
    {"name": "New York",     "latitude": 40.71280,  "longitude": -74.00600, "timezone": "America/New_York"},
    {"name": "Los Angeles",  "latitude": 34.05220,  "longitude": -118.24370, "timezone": "America/Los_Angeles"},
    {"name": "Chicago",      "latitude": 41.85003,  "longitude": -87.65005, "timezone": "America/Chicago"},
    {"name": "Houston",      "latitude": 29.76328,  "longitude": -95.36327, "timezone": "America/Chicago"},
    {"name": "Atlanta",      "latitude": 33.74900,  "longitude": -84.38798, "timezone": "America/New_York"},
    {"name": "London",       "latitude": 51.50853,  "longitude": -0.12574,  "timezone": "Europe/London"},
    {"name": "Paris",        "latitude": 48.85341,  "longitude":  2.34880,  "timezone": "Europe/Paris"},
    {"name": "Tokyo",        "latitude": 35.68950,  "longitude": 139.69171, "timezone": "Asia/Tokyo"},
    {"name": "Sydney",       "latitude": -33.86785, "longitude": 151.20732, "timezone": "Australia/Sydney"},
    {"name": "Nairobi",      "latitude": -1.28333,  "longitude":  36.81667, "timezone": "Africa/Nairobi"},
]

# ----------------------------------------------------------------
# GCS
# ----------------------------------------------------------------
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

def gcs_raw_key(city_name: str, run_date: str) -> str:
    """
    GCS object key for a raw API response.
    Pattern: raw/{city_name}/{run_date}.json
    Example: raw/New York/2024-03-10.json
    """
    safe_name = city_name.replace(" ", "_").lower()
    return f"raw/{safe_name}/{run_date}.json"

# ----------------------------------------------------------------
# Database
# ----------------------------------------------------------------
DB_CONFIG = {
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     int(os.getenv("POSTGRES_PORT", 5432)),
    "dbname":   os.getenv("POSTGRES_DB"),
    "user":     os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}