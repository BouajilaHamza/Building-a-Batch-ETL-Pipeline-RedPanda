import quixstreams as qx

from app.setup.config import settings
from app.setup.logging_config import setup_logging


def transform(msg):
    new_msg = []
    for i in range(len(msg["data"])):
        new_msg.append(msg["data"][i]["quote"]["USD"])
    return new_msg


class RedpandaInitializer:
    def __init__(self):
        self.logger = setup_logging("RedpandaInitializer")
        self.app = qx.Application(
            broker_address="localhost:9092",
            consumer_group="StreamingAppConsumerGroup",
            auto_offset_reset="earliest",
            loglevel="DEBUG",
        )

        self.input_topic = self.app.topic(
            settings.REDPANDA_TOPIC, value_deserializer="json"
        )
        self.output_topic = self.app.topic(
            "basic_cleaned_topic", value_serializer="json"
        )
        self.sdf = self.app.dataframe(self.input_topic)
        self.sdf = self.sdf.apply(transform)
        self.sdf.to_topic(self.output_topic)

    async def async_run(self):
        await self.app.run()
