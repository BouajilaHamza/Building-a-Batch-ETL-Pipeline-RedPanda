import quixstreams as qx
from quixstreams.models import TopicAdmin

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
        self.topic_admin = TopicAdmin(settings.REDPANDA_BROKER_ADDRESS)
        self.bitcoin_input_topic = self.app.topic(
            settings.REDPANDA_BITCOIN_INPUT_TOPIC, value_deserializer="json"
        )

        self.bitcoin_output_topic = self.app.topic(
            settings.REDPANDA_BITCOIN_OUTPUT_TOPIC, value_serializer="json"
        )

        self.news_input_topic = self.app.topic(
            settings.REDPANDA_NEWS_INPUT_TOPIC, value_deserializer="json"
        )
        self.news_output_topic = self.app.topic(
            settings.REDPANDA_NEWS_OUTPUT_TOPIC, value_serializer="json"
        )

    def clear_topics(self):
        # Delete the topics
        self.topic_admin.admin_client.delete_topics(
            [
                settings.REDPANDA_NEWS_INPUT_TOPIC,
                settings.REDPANDA_NEWS_OUTPUT_TOPIC,
                settings.REDPANDA_BITCOIN_INPUT_TOPIC,
                settings.REDPANDA_BITCOIN_OUTPUT_TOPIC,
            ]
        )

    def run(self):
        self.app.run()
