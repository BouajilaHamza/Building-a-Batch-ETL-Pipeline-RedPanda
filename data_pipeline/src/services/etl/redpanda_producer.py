import time
import uuid

from data_pipeline.src.services.etl.redpanda_init import RedpandaBase


class RedpandaProducer(RedpandaBase):
    def __init__(self):
        super().__init__()
        self.logger.name = "RedpandaProducer"

    def produce_data(self, data):
        # Create a Producer instance
        with self.app.get_producer() as producer:
            # Serialize an event using the defined Topic
            message = self.input_topic.serialize(key=str(uuid.uuid4()), value=data)
            # Produce a message into the Kafka topic
            self.logger.info(f"Producing message: {message}")
            producer.produce(
                topic=self.input_topic.name, value=message.value, key=message.key
            )
            time.sleep(2)
