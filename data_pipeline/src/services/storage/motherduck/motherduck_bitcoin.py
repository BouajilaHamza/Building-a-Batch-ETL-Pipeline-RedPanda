import json

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
                id INTEGER PRIMARY KEY,
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
                UNIQUE (id, last_updated)
            );
        """)

    def clean_data(self, raw_data: bytes | list) -> list:
        # Perform specific data cleaning and transformation
        if isinstance(raw_data, list):
            return raw_data
        data_str = raw_data.decode("utf-8")
        json_data = json.loads(data_str)
        return json_data

    def load_data(self, data: bytes | list):
        try:
            for raw_record in data:
                record = self.clean_data(raw_record)
                self.logger.debug(f"Loading record: {record}")
                for doc in record:
                    if doc:
                        self.conn.sql(
                            f"""INSERT INTO BitcoinData (id, price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, market_cap_dominance, fully_diluted_market_cap, last_updated)
                            VALUES (
                            '{doc["id"]}',
                            '{doc["price"]}',
                            '{doc["volume_24h"]}',
                            '{doc["volume_change_24h"]}',
                            '{doc["percent_change_1h"]}',
                            '{doc["percent_change_24h"]}',
                            '{doc["percent_change_7d"]}',
                            '{doc["market_cap"]}',
                            '{doc["market_cap_dominance"]}',
                            '{doc["fully_diluted_market_cap"]}',
                            '{doc["last_updated"]}'
                        )
                            ON CONFLICT (last_updated) DO NOTHING
                            """
                        )
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
            raise Exception(f"Error loading data: {e}")
