# test_kafka.py

from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v:
        json.dumps(v).encode()
)

producer.send(
    "city_events",
    {"test": "hello"}
)

producer.flush()

print("SUCCESS")

