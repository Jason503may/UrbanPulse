import time

import schedule
from loguru import logger

from producers.weather_producer import WeatherProducer
from producers.air_quality_producer import AirQualityProducer
from producers.traffic_producer import TrafficProducer

logger.add(
    "logs/scheduler.log",
    rotation="10 MB",
    retention="7 days"
)


def run_weather():

    logger.info(
        "Running Weather Producer"
    )

    WeatherProducer().run()


def run_air_quality():

    logger.info(
        "Running Air Quality Producer"
    )

    AirQualityProducer().run()


schedule.every(30).seconds.do(
    run_weather
)

schedule.every(30).seconds.do(
    run_air_quality
)

def run_traffic():

    logger.info(
        "Running Traffic Producer"
    )

    TrafficProducer().run()

schedule.every(30).seconds.do(
    run_traffic
)

def main():

    logger.info(
        "UrbanPulse Scheduler Started"
    )

    while True:

        schedule.run_pending()

        time.sleep(1)


if __name__ == "__main__":
    main()

