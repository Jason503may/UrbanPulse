from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"]
)

future = producer.send(
    "city_events",
    b"hello urbanpulse"
)

producer.flush()

metadata = future.get(timeout=10)

print(
    f"topic={metadata.topic}, "
    f"partition={metadata.partition}, "
    f"offset={metadata.offset}"
)

producer.close()


