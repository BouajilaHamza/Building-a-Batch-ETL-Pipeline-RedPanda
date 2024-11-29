import snowflake.connector

from app.setup.config import settings


class SnowflakeLoader:
    def __init__(self):
        self.conn = snowflake.connector.connect(
            user=settings.SNOWFLAKE_USERNAME,
            password=settings.SNOWFLAKE_PASSWORD,
            account=settings.SNOWFLAKE_ACCOUNT,
        )

        self.cursor = self.conn.cursor()

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
                (record["field1"], record["field2"]),
            )


class BitcoinDataLoader(SnowflakeLoader):
    def clean_data(self, raw_data):
        # Perform specific data cleaning and transformation
        # cleaned_data = []
        # for record in raw_data:
        #     cleaned_record = {
        #         'field1': record['field1'].lower(),
        #         'field2': record['field2'].replace(" ", "_")
        #     }
        #     cleaned_data.append(cleaned_record)
        return [raw_data]

    def load_data(self, data):
        cleaned_data = self.clean_data(data)
        for record in cleaned_data:
            self.cursor.execute(
                "INSERT INTO BitcoinData (field1, field2) VALUES (%s, %s)",
                (record["field1"], record["field2"]),
            )
