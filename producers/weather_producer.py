import uuid
from datetime import datetime, UTC

import requests
from tenacity import retry, stop_after_attempt, wait_fixed

from schemas.event import UrbanPulseEvent
from schemas.location import Location
from producers.base_producer import BaseProducer

from producers.tamilnadu_locations import (
    TAMILNADU_LOCATIONS
)


class WeatherProducer(BaseProducer):

    def __init__(self):

        super().__init__(
            "logs/weather.log"
        )

        self.output_file = (
            "data/raw/weather_events.jsonl"
        )

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(5)
    )
    def fetch_weather(
        self,
        latitude,
        longitude
    ):

        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={latitude}"
            f"&longitude={longitude}"
            "&current=temperature_2m,"
            "relative_humidity_2m,"
            "wind_speed_10m"
        )

        response = requests.get(
            url,
            timeout=10
        )

        response.raise_for_status()

        return response.json()

    def build_event(
        self,
        weather_data,
        district,
        latitude,
        longitude
    ):

        current = weather_data["current"]

        payload = {
            "temperature": current.get(
                "temperature_2m"
            ),
            "humidity": current.get(
                "relative_humidity_2m"
            ),
            "wind_speed": current.get(
                "wind_speed_10m"
            )
        }

        required_fields = [
            "temperature",
            "humidity",
            "wind_speed"
        ]

        for field in required_fields:

            if payload[field] is None:

                raise ValueError(
                    f"{field} missing from API response"
                )

        event = UrbanPulseEvent(
            event_id=str(uuid.uuid4()),
            event_type="weather",
            event_timestamp=datetime.now(UTC),
            city=district,
            source="open_meteo",
            location=Location(
                latitude=latitude,
                longitude=longitude
            ),
            payload=payload
        )

        return event

    def run(self):

        for location in TAMILNADU_LOCATIONS:

            try:

                weather_data = (
                    self.fetch_weather(
                        location["latitude"],
                        location["longitude"]
                    )
                )

                event = self.build_event(
                    weather_data,
                    location["district"],
                    location["latitude"],
                    location["longitude"]
                )

                self.save_event(
                    event,
                    self.output_file
                )

                self.logger.info(
                    f"Saved weather for "
                    f"{location['district']}"
                )

            except Exception as e:

                self.logger.error(
                    f"{location['district']} failed: {e}"
                )
import time

def main():

    producer = WeatherProducer()

    while True:

        producer.run()

        time.sleep(60)


if __name__ == "__main__":
    main()

