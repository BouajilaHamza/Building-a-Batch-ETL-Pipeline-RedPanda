import json
from datetime import datetime

from pydantic import BaseModel

from data_pipeline.src.services.storage.motherduck.motherduck_init import (
    MotherduckLoader,
)


class BitcoinData(BaseModel):
    id: str
    price: float
    volume_24h: float
    volume_change_24h: float
    percent_change_1h: float
    percent_change_24h: float
    percent_change_7d: float
    market_cap: float
    market_cap_dominance: float
    fully_diluted_market_cap: float
    last_updated: datetime


class BitcoinDataLoader(MotherduckLoader):
    def __init__(self):
        super().__init__()
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS BitcoinData (
                id VARCHAR(255) PRIMARY KEY,
                price FLOAT,
                volume_24h FLOAT,
                volume_change_24h FLOAT,
                percent_change_1h FLOAT,
                percent_change_24h FLOAT,
                percent_change_7d FLOAT,
                market_cap FLOAT,
                market_cap_dominance FLOAT,
                fully_diluted_market_cap FLOAT,
                last_updated DATETIME,
            );
        """)

    def clean_data(self, raw_data: bytes | list) -> list:
        if isinstance(raw_data, list):
            return raw_data

        data_str = raw_data.decode("utf-8")
        json_data = json.loads(data_str)
        return json_data

    def load_data(self, data: bytes | list):
        try:
            record = self.clean_data(data)
            for doc in record:
                doc = BitcoinData(**doc)
                query = """INSERT INTO BitcoinData VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? );"""
                values = [
                    doc.id,
                    doc.price,
                    doc.volume_24h,
                    doc.volume_change_24h,
                    doc.percent_change_1h,
                    doc.percent_change_24h,
                    doc.percent_change_7d,
                    doc.market_cap,
                    doc.market_cap_dominance,
                    doc.fully_diluted_market_cap,
                    doc.last_updated.isoformat(),
                ]
                self.conn.execute(query, values)
                self.logger.info("Data loaded successfully")
        except Exception as e:
            self.logger.info(f"Error loading data: {e}")
            raise Exception(f"Error loading data: {e}")

    def close(self):
        self.conn.close()
