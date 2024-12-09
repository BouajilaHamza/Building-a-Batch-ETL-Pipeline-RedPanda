from app.data_ingestion.data_ingestor import DataIngestor
from app.etl.redpanda_consumer import RedpandaConsumer
from app.etl.redpanda_producer import RedpandaProducer
from app.setup.logging_config import setup_logging
from app.storage.snowflake import SnowflakeLoader


class ETLPipeline:
    def __init__(
        self,
        ingestors: list[DataIngestor],
        producer: RedpandaProducer,
        consumer: RedpandaConsumer,
        dataloader: SnowflakeLoader,
        batch_size: int = 100,
    ):
        self.ingestors = ingestors
        self.producer = producer
        self.consumer = consumer
        self.loader = dataloader
        self.batch_size = batch_size
        self.batch = []
        self.logger = setup_logging("ETLPipeline")

    # def run(self):
    #     while True:
    #         for ingestor in self.ingestors:
    #             data = ingestor.fetch_data()
    #             self.producer.produce_data(data)
    #             consumed_msg = self.consumer.consume_data()
    #             self.loader.load_data(consumed_msg)

    def run(self):
        try:
            while True:
                # Fetch data from ingestors
                for ingestor in self.ingestors:
                    try:
                        data = ingestor.fetch_data()
                        self.producer.produce_data(data)  # Produce data to Redpanda

                        # Consume data from Redpanda
                        message = self.consumer.consume_data()
                        if message:
                            self.batch.append(message["Value"])
                            self.logger.debug("Btach size: " + str(len(self.batch)))
                            if len(self.batch) >= self.batch_size:
                                self.loader.load_data(
                                    self.batch
                                )  # Load batch data to Snowflake
                                self.batch = []
                    except Exception as e:
                        self.logger.error(f"Error processing data: {e}")

        except KeyboardInterrupt:
            if self.batch:
                self.loader.load_data(self.batch)
            self.logger.info("ETL pipeline stopped by user.")
        except Exception as e:
            self.logger.error(f"Unexpected error: {e}")
