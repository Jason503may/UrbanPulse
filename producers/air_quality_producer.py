import time
import uuid

from datetime import datetime, UTC

import requests

from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed
)

from schemas.event import UrbanPulseEvent
from schemas.location import Location

from producers.base_producer import BaseProducer

from producers.tamilnadu_locations import (
    TAMILNADU_LOCATIONS
)

from kafka_utils.producer import publish_event


class AirQualityProducer(BaseProducer):

    def __init__(self):

        super().__init__(
            "logs/air_quality.log"
        )

        self.output_file = (
            "data/raw/air_quality_events.jsonl"
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5)
    )
    def fetch_air_quality(
        self,
        latitude,
        longitude
    ):

        url = (
            "https://air-quality-api.open-meteo.com/v1/air-quality"
            f"?latitude={latitude}"
            f"&longitude={longitude}"
            "&current=pm10,pm2_5,carbon_monoxide,nitrogen_dioxide"
        )

        response = requests.get(
            url,
            timeout=10
        )

        response.raise_for_status()

        return response.json()

    def build_event(
        self,
        city,
        latitude,
        longitude,
        aq_data
    ):

        current = aq_data["current"]

        payload = {
            "pm10": current.get("pm10"),
            "pm25": current.get("pm2_5"),
            "carbon_monoxide": current.get(
                "carbon_monoxide"
            ),
            "nitrogen_dioxide": current.get(
                "nitrogen_dioxide"
            )
        }

        return UrbanPulseEvent(
            event_id=str(uuid.uuid4()),
            event_type="air_quality",
            event_timestamp=datetime.now(UTC),
            city=city,
            source="open_meteo_air",
            location=Location(
                latitude=latitude,
                longitude=longitude
            ),
            payload=payload
        )

    def run(self):

        for location in TAMILNADU_LOCATIONS:

            city = location["district"]
            latitude = location["latitude"]
            longitude = location["longitude"]

            try:

                aq_data = self.fetch_air_quality(
                    latitude,
                    longitude
                )

                event = self.build_event(
                    city,
                    latitude,
                    longitude,
                    aq_data
                )

                self.save_event(
                    event,
                    self.output_file
                )

                publish_event(
                    event.model_dump()
                )

                self.logger.info(
                    f"Saved + Published air quality for {city}"
                )

            except Exception as e:

                self.logger.error(
                    f"{city} failed: {e}"
                )

            time.sleep(1)


def main():

    producer = AirQualityProducer()

    while True:

        producer.run()

        time.sleep(60)


if __name__ == "__main__":
    main()


        

