import random
import time
import uuid

from datetime import datetime, UTC

from schemas.event import UrbanPulseEvent
from schemas.location import Location

from producers.base_producer import BaseProducer

from producers.tamilnadu_locations import (
    TAMILNADU_LOCATIONS
)

from kafka_utils.producer import publish_event


ROADS = [
    "Highway",
    "Bus Stand",
    "Market Road",
    "Railway Junction",
    "City Center"
]


class TrafficProducer(BaseProducer):

    def __init__(self):

        super().__init__(
            "logs/traffic.log"
        )

        self.output_file = (
            "data/raw/traffic_events.jsonl"
        )

    def generate_traffic_data(self):

        return {
            "road_name": random.choice(
                ROADS
            ),
            "traffic_density": random.randint(
                10,
                100
            ),
            "avg_speed": random.randint(
                5,
                80
            ),
            "incident": random.choice(
                [True, False, False]
            )
        }

    def build_event(
        self,
        city,
        latitude,
        longitude,
        traffic_data
    ):

        return UrbanPulseEvent(
            event_id=str(uuid.uuid4()),
            event_type="traffic",
            event_timestamp=datetime.now(UTC),
            city=city,
            source="traffic_simulator",
            location=Location(
                latitude=latitude,
                longitude=longitude
            ),
            payload=traffic_data
        )

    def run(self):

        for location in TAMILNADU_LOCATIONS:

            city = location["district"]
            latitude = location["latitude"]
            longitude = location["longitude"]

            try:

                traffic_data = (
                    self.generate_traffic_data()
                )

                event = self.build_event(
                    city,
                    latitude,
                    longitude,
                    traffic_data
                )

                self.save_event(
                    event,
                    self.output_file
                )

                publish_event(
                    event.model_dump()
                )

                self.logger.info(
                    f"Saved + Published traffic for {city}"
                )

            except Exception as e:

                self.logger.error(
                    f"{city} failed: {e}"
                )

            time.sleep(1)


def main():

    producer = TrafficProducer()

    while True:

        producer.run()

        time.sleep(30)


if __name__ == "__main__":
    main()


