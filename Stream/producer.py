# producer.py
import requests
import json
import time
from kafka import KafkaProducer
from config import *


# وحطه بدل منه
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8")
)

def fetch_flights():
    url = "https://opensky-network.org/api/states/all"
    params = {
        "lamin": NY_BBOX["lamin"],
        "lamax": NY_BBOX["lamax"],
        "lomin": NY_BBOX["lomin"],
        "lomax": NY_BBOX["lomax"]
    }
    try:
        res = requests.get(url, auth=(OPENSKY_USERNAME, OPENSKY_PASSWORD), params=params)
        data = res.json()
        flights = []
        if data and data.get("states"):
            for s in data["states"]:
                flight = {
                    "icao24": s[0],
                    "callsign": s[1].strip() if s[1] else None,
                    "origin_country": s[2],
                    "longitude": s[5],
                    "latitude": s[6],
                    "altitude": s[7],
                    "velocity": s[9],
                    "heading": s[10],
                    "on_ground": s[8],
                    "timestamp": s[3]
                }
                flights.append(flight)
        return flights
    except Exception as e:
        print(f"OpenSky error: {e}")
        return []

def fetch_weather():
    results = []
    for airport, coords in NY_AIRPORTS.items():
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current": "temperature_2m,wind_speed_10m,precipitation,visibility,weathercode",
            "wind_speed_unit": "ms"
        }
        try:
            res = requests.get(url, params=params)
            data = res.json()
            current = data["current"]
            weather = {
                "airport": airport,
                "latitude": coords["lat"],
                "longitude": coords["lon"],
                "temperature": current["temperature_2m"],
                "wind_speed": current["wind_speed_10m"],
                "precipitation": current["precipitation"],
                "weathercode": current["weathercode"],
                "timestamp": current["time"]
            }
            results.append(weather)
        except Exception as e:
            print(f"Weather error for {airport}: {e}")
    return results

print("Producer started...")

while True:
    # fetch وبعت flights
    flights = fetch_flights()
    for flight in flights:
        producer.send(TOPICS["flights"], flight)
    print(f"Sent {len(flights)} flights to Kafka")

    # fetch وبعت weather
    weather_list = fetch_weather()
    for w in weather_list:
        producer.send(TOPICS["weather"], w)
    print(f"Sent {len(weather_list)} weather records to Kafka")

    producer.flush()
    time.sleep(60)  # كل دقيقة