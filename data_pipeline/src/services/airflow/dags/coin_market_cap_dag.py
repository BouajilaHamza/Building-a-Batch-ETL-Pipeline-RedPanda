from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from data_pipeline.src.core.logging_config import setup_logging
from data_pipeline.src.services.data_ingestion.coin_market_cap_ingestor import (
    CoinMarketCapIngestor,
)
from data_pipeline.src.services.etl.redpanda_consumer import RedpandaConsumer
from data_pipeline.src.services.etl.redpanda_producer import RedpandaProducer
from data_pipeline.src.services.storage.snowflake import SnowflakeLoader

default_args = {
    "owner": "hamza",
    "depends_on_past": False,
    "start_date": datetime(2024, 12, 27),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


logger = setup_logging("ETLPipeline")


def fetch_and_produce_data():
    try:
        ingestor = CoinMarketCapIngestor()
        producer = RedpandaProducer()
        data = ingestor.fetch_data()
        producer.produce_data(data)
    except Exception as e:
        logger.error(f"Error in fetch_and_produce_data: {e}")
        raise


def consume_and_load_data():
    try:
        consumer = RedpandaConsumer()
        loader = SnowflakeLoader()
        batch_size = 100
        batch = []
        message = consumer.consume_data()
        if message:
            batch.append(message["Value"])
            logger.debug("Batch size: " + str(len(batch)))
            if len(batch) >= batch_size:
                loader.load_data(batch)
                batch = []
        if batch:
            loader.load_data(batch)
    except Exception as e:
        logger.error(f"Error in consume_and_load_data: {e}")
        raise


with DAG(
    "CoinMarketCapIngestorDag",
    default_args=default_args,
    description="ETL pipeline for CoinMarketCapIngestor using Airflow",
    schedule_interval=timedelta(days=1),
) as dag:
    fetch_produce_task = PythonOperator(
        task_id="fetch_and_produce_data",
        python_callable=fetch_and_produce_data,
    )

    consume_load_task = PythonOperator(
        task_id="consume_and_load_data",
        python_callable=consume_and_load_data,
    )

    fetch_produce_task >> consume_load_task
