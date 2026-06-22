import json
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(
        v,
        default=str
    ).encode("utf-8")
)


def publish_event(event):

    future = producer.send(
        "city_events",
        event
    )

    producer.flush()

    metadata = future.get(timeout=10)

    print(
        f"Sent to {metadata.topic} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )




