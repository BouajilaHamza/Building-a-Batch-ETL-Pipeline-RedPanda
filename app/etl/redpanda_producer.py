import quixstreams as qx
from app.config.setup import settings

class RedpandaProducer:
    def __init__(self):
        self.app = qx.Application(
            broker_address="localhost:9092",
        )
        self.topic = self.app.topic(settings.REDPANDA_TOPIC, value_serializer='json')


    def produce_data(self, data):
        # Create a Producer instance
        with self.app.get_producer() as producer:

            # Serialize an event using the defined Topic 
            message = self.topic.serialize(key="1", value=data)

            # Produce a message into the Kafka topic
            producer.produce(
                topic=self.topic.name, value=message.value, key=message.key
            )
