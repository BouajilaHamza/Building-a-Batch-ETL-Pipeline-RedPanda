import pandas as pd
import snowflake.connector

from app.setup.config import settings
from app.setup.logging_config import setup_logging


class SnowflakeLoader:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user=settings.SNOWFLAKE_USERNAME,
            password=settings.SNOWFLAKE_PASSWORD,
            account=settings.SNOWFLAKE_ACCOUNT,
        )
        self.logger = setup_logging("SnowflakeLoader")
        self.cursor = self.conn.cursor()
        self.cursor.execute("CREATE DATABASE IF NOT EXISTS StreamingAppDatabase")
        self.cursor.execute("USE DATABASE StreamingAppDatabase")

    def clean_data(self, raw_data):
        raise NotImplementedError("This method should be overridden by subclasses")

    def load_data(self, data):
        raise NotImplementedError("This method should be overridden by subclasses")

    def close_connection(self):
        self.conn.close()


class NewsDataLoader(SnowflakeLoader):
    def clean_data(self, raw_data):
        # Perform specific data cleaning and transformation
        # cleaned_data = []
        # for record in raw_data:
        #     cleaned_record = {
        #         'field1': record['field1'].upper(),
        #         'field2': record['field2'].strip()
        #     }
        #     cleaned_data.append(cleaned_record)
        return [raw_data]

    def load_data(self, data):
        cleaned_data = self.clean_data(data)
        for record in cleaned_data:
            self.cursor.execute(
                "INSERT INTO NewsData (field1, field2) VALUES (%s, %s)",
                (pd.DataFrame(record["field1"]), pd.DataFrame(record["field2"])),
            )


class BitcoinDataLoader(SnowflakeLoader):
    def __init__(self):
        super().__init__()
        self.cursor.execute(
            "CREATE TABLE IF NOT EXISTS BitcoinData (price VARCHAR(255), volume_24h VARCHAR(255), volume_change_24h VARCHAR(255), percent_change_1h VARCHAR(255), percent_change_24h VARCHAR(255), percent_change_7d VARCHAR(255), market_cap VARCHAR(255), market_cap_dominance VARCHAR(255), fully_diluted_market_cap VARCHAR(255), last_updated VARCHAR(255))"
        )

    def clean_data(self, raw_data):
        # Perform specific data cleaning and transformation
        cleaned_data = []
        for i in range(len(raw_data["data"])):
            cleaned_data.append(raw_data["data"][i]["quote"]["USD"])
        return cleaned_data

    def load_data(self, data):
        try:
            cleaned_data = self.clean_data(data)
            for record in cleaned_data:
                requette = """ INSERT INTO BitcoinData (price, volume_24h, volume_change_24h, percent_change_1h, percent_change_24h, percent_change_7d, market_cap, market_cap_dominance, fully_diluted_market_cap, last_updated) VALUES ({}, {}, {}, {}, {}, {}, {}, {}, {}, '{}')
                """.format(
                    record["price"],
                    record["volume_24h"],
                    record["volume_change_24h"],
                    record["percent_change_1h"],
                    record["percent_change_24h"],
                    record["percent_change_7d"],
                    record["market_cap"],
                    record["market_cap_dominance"],
                    record["fully_diluted_market_cap"],
                    record["last_updated"],
                )
                self.cursor.execute(requette)
        except Exception as e:
            self.logger.error(f"Error loading data: {e}")
