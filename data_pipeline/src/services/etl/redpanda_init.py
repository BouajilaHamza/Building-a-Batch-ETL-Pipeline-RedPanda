import quixstreams as qx

from data_pipeline.src.core.config import settings
from data_pipeline.src.core.logging_config import setup_logging


class RedpandaBase:
    def __init__(self):
        self.logger = setup_logging("RedpandaBase")
        self.app = qx.Application(
            broker_address=settings.REDPANDA_BROKER_ADDRESS,
            consumer_group="StreamingAppConsumerGroup",
            auto_offset_reset="earliest",
            loglevel=settings.LOG_LEVEL,
        )
        self.input_topic = self.app.topic(
            settings.REDPANDA_INPUT_TOPIC, value_deserializer="json"
        )
        self.output_topic = self.app.topic(
            settings.REDPANDA_OUTPUT_TOPIC, value_serializer="json"
        )

    def run(self):
        self.app.run()
