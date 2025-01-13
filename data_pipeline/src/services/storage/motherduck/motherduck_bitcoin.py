import json

from data_pipeline.src.schemas.bitcoin_schemas import BitcoinData
from data_pipeline.src.services.storage.motherduck.motherduck_init import (
    MotherduckLoader,
)


class BitcoinDataLoader(MotherduckLoader):
    def __init__(self):
        super().__init__()
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS BitcoinData (
                id VARCHAR(255),
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

    def clean_data(self, batch: list) -> BitcoinData:
        cleaned_list = []
        for message in batch:
            data_str = message["Value"].decode("utf-8")
            json_data = json.loads(data_str)
            cleaned_list.extend(json_data)
        cleaned_data = BitcoinData(root=cleaned_list)
        return cleaned_data

    def load_data(self, data: bytes | list):
        try:
            record = self.clean_data(data)
            for doc in record.root:
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
                self.logger.info(f"Data loaded successfully {doc}")
        except Exception as e:
            self.logger.info(f"Error loading data: {e}")
            raise Exception(f"Error loading data: {e}")

    def close(self):
        self.conn.close()
