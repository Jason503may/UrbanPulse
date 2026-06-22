import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "city_events",
    bootstrap_servers="localhost:29092",
    auto_offset_reset="earliest",
    value_deserializer=lambda x: json.loads(
        x.decode("utf-8")
    )
)


while True:

    records = consumer.poll(timeout_ms=5000)

    if not records:
        print("waiting for messages...")
        continue

    for _, messages in records.items():

        for message in messages:

            event = message.value

            print(event)



