from app.data_ingestion.coin_market_cap_ingestor import CoinMarketCapIngestor
from app.data_ingestion.news_data_ingestor import NewsDataIngestor
from app.etl.etl_pipeline import ETLPipeline
from app.etl.redpanda_consumer import RedpandaConsumer
from app.etl.redpanda_producer import RedpandaProducer
from app.storage.snowflake import BitcoinDataLoader

# ,NewsDataLoader

if __name__ == "__main__":
    news_ingestor = NewsDataIngestor()
    coin_ingestor = CoinMarketCapIngestor()

    producer = RedpandaProducer()
    consumer = RedpandaConsumer()

    bitcoin_loader = BitcoinDataLoader()
    # news_loader = NewsDataLoader()

    bitcoin_pipeline = ETLPipeline([coin_ingestor], producer, consumer, bitcoin_loader)
    # news_pipeline = ETLPipeline([news_ingestor], producer,consumer,news_loader)

    bitcoin_pipeline.run()
    # news_pipeline.run()
