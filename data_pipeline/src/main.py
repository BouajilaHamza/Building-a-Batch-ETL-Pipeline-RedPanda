from data_pipeline.src.data_ingestion.coin_market_cap_ingestor import (
    CoinMarketCapIngestor,
)
from data_pipeline.src.etl.etl_pipeline import ETLPipeline
from data_pipeline.src.etl.redpanda_consumer import RedpandaConsumer
from data_pipeline.src.etl.redpanda_producer import RedpandaProducer
from data_pipeline.src.storage.snowflake import BitcoinDataLoader


# ,NewsDataLoader
def main():
    # news_ingestor = NewsDataIngestor()
    coin_ingestor = CoinMarketCapIngestor()
    producer = RedpandaProducer()
    consumer = RedpandaConsumer()

    bitcoin_loader = BitcoinDataLoader()
    # news_loader = NewsDataLoader()

    bitcoin_pipeline = ETLPipeline([coin_ingestor], producer, consumer, bitcoin_loader)
    # news_pipeline = ETLPipeline([news_ingestor], producer,consumer,news_loader)

    bitcoin_pipeline.run()
    # news_pipeline.run()


if __name__ == "__main__":
    main()
