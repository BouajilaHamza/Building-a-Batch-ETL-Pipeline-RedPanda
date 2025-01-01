import json

import duckdb

from data_pipeline.src.core.config import settings
from data_pipeline.src.core.logging_config import setup_logging


class MotherduckLoader:
    def __init__(self):
        self.logger = setup_logging("MotherduckLoader")
        self.conn = self.connect_to_database()

    def connect_to_database(self):
        try:
            # First try to connect to check if database exists
            conn = duckdb.connect(
                f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
            )
            self.logger.info(
                f"Successfully connected to existing database {settings.DATABASE_NAME}"
            )
            return conn
        except Exception as e:
            if "no database/share named" in str(e):
                self.logger.info(
                    f"Database {settings.DATABASE_NAME} not found. Creating new database..."
                )
                # Connect to MotherDuck without specifying database
                temp_conn = duckdb.connect(
                    f"md:?motherduck_token={settings.MOTHERDUCK_TOKEN}"
                )
                # Create the database
                temp_conn.sql(f"CREATE DATABASE IF NOT EXISTS {settings.DATABASE_NAME}")
                temp_conn.close()
                # Now connect to the newly created database
                conn = duckdb.connect(
                    f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
                )
                self.logger.info(
                    f"Successfully created and connected to database {settings.DATABASE_NAME}"
                )
                return conn
            else:
                self.logger.error(f"Failed to connect to MotherDuck: {str(e)}")
                raise

    def clean_data(self, raw_data: bytes | list) -> list:
        raise NotImplementedError("This method should be overridden by subclasses")

    def load_data(self, data: bytes):
        raise NotImplementedError("This method should be overridden by subclasses")

    def close_connection(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            self.logger.info("Connection closed successfully")


class NewsDataLoader(MotherduckLoader):
    def clean_data(self, raw_data):
        # Perform specific data cleaning and transformation
        return [raw_data]

    def load_data(self, data):
        cleaned_data = self.clean_data(data)
        for record in cleaned_data:
            self.conn.sql(
                "INSERT INTO NewsData (field1, field2) VALUES (?, ?)",
                (record["field1"], record["field2"]),
            )


class BitcoinDataLoader(MotherduckLoader):
    def __init__(self):
        super().__init__()
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS BitcoinData (
                price VARCHAR(255),
                volume_24h VARCHAR(255),
                volume_change_24h VARCHAR(255),
                percent_change_1h VARCHAR(255),
                percent_change_24h VARCHAR(255),
                percent_change_7d VARCHAR(255),
                market_cap VARCHAR(255),
                market_cap_dominance VARCHAR(255),
                fully_diluted_market_cap VARCHAR(255),
                last_updated VARCHAR(255)
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
                self.logger.info(f"Loading record: {record}")
                for doc in record:
                    self.conn.sql(
                        f"""INSERT INTO BitcoinData (price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, market_cap_dominance, fully_diluted_market_cap, last_updated)
                        VALUES (
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
                        """
                    )
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
