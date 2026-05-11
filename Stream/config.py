# config.py

OPENSKY_USERNAME = "ahmedrefat1412@gmail.com"
OPENSKY_PASSWORD = "fWCwa6X**Y28nUV"

KAFKA_BROKER = "localhost:9092"

TOPICS = {
    "flights": "flight_raw",
    "weather": "weather_raw"
}

# مطارات نيويورك
NY_AIRPORTS = {
    "JFK": {"lat": 40.6413, "lon": -73.7781},
    "LGA": {"lat": 40.7769, "lon": -73.8740},
    "EWR": {"lat": 40.6895, "lon": -74.1745}
}

# Bounding box نيويورك للـ OpenSky
NY_BBOX = {
    "lamin": 40.4,
    "lamax": 41.0,
    "lomin": -74.5,
    "lomax": -73.5
}