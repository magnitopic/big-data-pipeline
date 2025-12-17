import requests
from config.api_keys import APIConfig

def test_openweather():
    url = f"{APIConfig.OPENWEATHER_BASE_URL}/weather"
    params = {
        "lat": APIConfig.DEFAULT_LAT,
        "lon": APIConfig.DEFAULT_LON,
        "appid": APIConfig.OPENWEATHER_API_KEY
    }
    r = requests.get(url, params=params)
    print("OpenWeather status:", r.status_code)
    print(r.json())

def test_opensky():
    url = f"{APIConfig.OPENSKY_BASE_URL}/states/all"
    r = requests.get(url, auth=(APIConfig.OPENSKY_USER, APIConfig.OPENSKY_PASSWORD))
    print("OpenSky status:", r.status_code)
    print(r.json())
    