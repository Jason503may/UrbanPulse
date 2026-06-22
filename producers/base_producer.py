from loguru import logger

from kafka_utils.producer import publish_event


class BaseProducer:

    def __init__(self, log_file: str):

        logger.add(
            log_file,
            rotation="10 MB",
            retention="7 days",
            level="INFO"
        )

        self.logger = logger

    def save_event(self, event, output_file):

        event_json = event.model_dump(
                mode="json"

                )


        publish_event(
            event_json
        )

        self.logger.info(
            f"Published event: {event.event_id}"
        )


