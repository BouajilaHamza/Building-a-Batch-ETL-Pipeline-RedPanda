import quixstreams as qx

class RedpandaProducer:
    def __init__(self):
        self.app = qx.Application(
            broker_address="localhost:9092",
        )
        self.topic = self.app.topic("my_topic")
        self.topic = self.app.topic(name='my_topic', value_serializer='json')


    def produce_data(self, data):
        event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}

        # Create a Producer instance
        with self.app.get_producer() as producer:

            # Serialize an event using the defined Topic 
            message = self.topic.serialize(key=event["id"], value=event)

            # Produce a message into the Kafka topic
            producer.produce(
                topic=self.topic.name, value=message.value, key=message.key
            )
