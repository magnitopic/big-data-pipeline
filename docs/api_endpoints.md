# API Endpoints – OpenSky & OpenWeather

## OpenSky (Air Traffic)
Base URL: https://opensky-network.org/api

### Get arrivals
GET /flights/arrival
params: airport, begin, end

### Get departures
GET /flights/departure
params: airport, begin, end

### Get all aircraft states
GET /states/all


## OpenWeather (Free Tier – API 2.5)
Base URL: https://api.openweathermap.org/data/2.5

### Current weather
GET /weather
params: lat, lon, appid

### 5-day forecast
GET /forecast
params: lat, lon, appid
