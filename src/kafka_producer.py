import json, time, random
from datetime import datetime
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"))

BASE = {"Delhi": 32, "Mumbai": 30, "Chennai": 33, "Bengaluru": 26, "Hyderabad": 31}

print("Producer started. Press Ctrl+C to stop.")
count = 0
while True:
    for city, base in BASE.items():
        event = {
            "city": city,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "temperature": round(random.gauss(base, 2), 1),
            "humidity": round(random.uniform(30, 95), 1),
            "rainfall": round(max(0, random.gauss(0.3, 1)), 2),
            "wind_speed": round(random.uniform(2, 30), 1),
        }
        producer.send("weather-events", event)
        count += 1
    producer.flush()
    print(f"Sent {count} events", end="\r")
    time.sleep(2)