from data_ingestion.news_data_ingestor import NewsDataIngestor
from data_ingestion.coin_market_cap_ingestor import CoinMarketCapIngestor
from etl.etl_pipeline import ETLPipeline
from etl.redpanda_producer import RedpandaProducer

if __name__ == "__main__":
    news_ingestor = NewsDataIngestor()
    coin_ingestor = CoinMarketCapIngestor()
    ingestors = [news_ingestor, coin_ingestor]
    producer = RedpandaProducer()
    pipeline = ETLPipeline(ingestors, producer)
    pipeline.run()
