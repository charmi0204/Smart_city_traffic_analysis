from kafka import KafkaProducer
import json
import random
import time
from datetime import datetime

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x).encode("utf-8")
)

locations = [
    "City Center",
    "Railway Station",
    "Bus Stand",
    "Airport Road",
    "Market Area",
    "University Road"
]

services = [
    "traffic",
    "public_transport",
    "parking",
    "road_sensor"
]

vehicles = [
    "Car",
    "Bus",
    "Auto",
    "Bike",
    "Taxi"
]

print("Smart City Producer Started...")

event_id = 1

while True:

    service = random.choice(services)
    location = random.choice(locations)

    event = {
        "event_id": event_id,
        "service": service,
        "location": location,
        "vehicle_type": random.choice(vehicles),
        "vehicle_count": random.randint(10, 100),
        "speed": round(random.uniform(10, 60), 2),
        "passenger_count": random.randint(5, 80),
        "temperature": round(random.uniform(20, 40), 2),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    producer.send("smart_city", event)
    producer.flush()

    print(event)

    event_id += 1
    time.sleep(2)
