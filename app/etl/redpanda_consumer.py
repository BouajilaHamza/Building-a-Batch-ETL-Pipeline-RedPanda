from app.etl.redpanda_init import RedpandaInitializer


class RedpandaConsumer(RedpandaInitializer):
    def __init__(self):
        super().__init__()
        self.logger.name = "RedpandaConsumer"

    def consume_data(self):
        # Create a Consumer instance
        with self.app.get_consumer() as consumer:
            consumer.subscribe([self.output_topic.name])
            self.logger.info(f"Consumer Subscribed to topic: {self.output_topic.name}")
            message = consumer.poll(timeout=10)

            if message is None:
                print("Waiting ...")
                self.logger.info("Waiting ...")
            elif message.error() is not None:
                self.logger.error(f"Error: {message.error()}")
                print(f"Error: {message.error()}")
                raise Exception(f"Error: {message.error()}")
            else:
                self.logger.info(
                    f"Received message: ,Key : {message.key()} \t Value : {message.value()}"
                )
                return {"Key": message.key(), "Value": message.value()}
                # Add any processing logic for the consumed messages here
