import quixstreams as qx
from app.config.setup import settings
from app.config.logging_config import setup_logging

class RedpandaConsumer:
    def __init__(self):
        self.app = qx.Application(
            broker_address="localhost:9092",
        )
        self.topic = self.app.topic(settings.REDPANDA_TOPIC, value_deserializer='json')
        self.logger = setup_logging("RedpandaConsumer")

    def consume_data(self):
        # Create a Consumer instance
        with self.app.get_consumer() as consumer:
            consumer.subscribe([self.topic])
            message  = consumer.poll(timeout=2)
            if message is not None:
                self.logger.info(f"Received message: Key={message.key}, Value={message.value}")
                # Add any processing logic for the consumed messages here
