import os
from dotenv import load_dotenv

load_dotenv()

class APIConfig:
    OPENSKY_USER = os.getenv("OPENSKY_USER")
    OPENSKY_PASSWORD = os.getenv("OPENSKY_PASSWORD")

    OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
    OPENWEATHER_BASE_URL = os.getenv("OPENWEATHER_BASE_URL")

    DEFAULT_AIRPORT = os.getenv("DEFAULT_AIRPORT")
    DEFAULT_LAT = float(os.getenv("DEFAULT_LAT"))
    DEFAULT_LON = float(os.getenv("DEFAULT_LON"))
