from app.data_ingestion.data_ingestor import DataIngestor
from app.etl.redpanda_consumer import RedpandaConsumer
from app.etl.redpanda_producer import RedpandaProducer
from app.storage.snowflake import SnowflakeLoader


class ETLPipeline:
    def __init__(
        self,
        ingestors: list[DataIngestor],
        producer: RedpandaProducer,
        consumer: RedpandaConsumer,
        dataloader: SnowflakeLoader,
    ):
        self.ingestors = ingestors
        self.producer = producer
        self.consumer = consumer
        self.loader = dataloader

    def run(self):
        while True:
            for ingestor in self.ingestors:
                data = ingestor.fetch_data()
                transformed_data = self.transform(data)
                self.producer.produce_data(transformed_data)
                self.consumer.consume_data()
                self.loader.load_data(transformed_data)

    def transform(self, data):
        # Perform data transformation here
        return data
