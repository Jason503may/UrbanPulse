from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "city_events",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest"
)

for msg in consumer:
    print(msg.value)

