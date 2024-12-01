import quixstreams as qx

from app.setup.config import settings
from app.setup.logging_config import setup_logging


class RedpandaInitializer:
    def __init__(self):
        self.logger = setup_logging("RedpandaInitializer")
        self.app = qx.Application(
            broker_address="localhost:9092", consumer_group="StreamingAppConsumerGroup"
        )
        self.topic = self.app.topic(settings.REDPANDA_TOPIC, value_deserializer="json")
        self.logger.info("RedpandaInitializer initialized")
        self.logger.info(f"Topic name: {self.topic.name}")
