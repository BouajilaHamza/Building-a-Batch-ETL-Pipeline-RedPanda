from app.data_ingestion.news_data_ingestor import NewsDataIngestor
from app.data_ingestion.coin_market_cap_ingestor import CoinMarketCapIngestor
from app.etl.etl_pipeline import ETLPipeline
from app.etl.redpanda_producer import RedpandaProducer
from app.etl.redpanda_consumer import RedpandaConsumer

if __name__ == "__main__":
    news_ingestor = NewsDataIngestor()
    coin_ingestor = CoinMarketCapIngestor()
    ingestors = [news_ingestor, coin_ingestor]
    producer = RedpandaProducer()
    consumer = RedpandaConsumer()
    pipeline = ETLPipeline(ingestors, producer,consumer)
    
    pipeline.run()
