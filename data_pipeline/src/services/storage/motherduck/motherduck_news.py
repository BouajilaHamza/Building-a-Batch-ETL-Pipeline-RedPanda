import json

from data_pipeline.src.core.logging_config import setup_logging
from data_pipeline.src.services.storage.motherduck.motherduck_init import (
    MotherduckLoader,
)


class NewsDataLoader(MotherduckLoader):
    def __init__(self):
        super().__init__()
        self.create_table_if_not_exists()
        self.logger = setup_logging("NewsDataLoader")

    def create_table_if_not_exists(self):
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS NewsData (
                description VARCHAR(255),
                source VARCHAR(255),
                pubDate VARCHAR(255),
                title VARCHAR(255),
            );
        """)

    def clean_data(self, raw_data: bytes | list) -> list:
        if isinstance(raw_data, list):
            cleaned_data = [
                json.loads(doc.decode("utf-8"))
                for doc in raw_data
                if isinstance(doc, bytes)
            ]
            return cleaned_data
        data_str = raw_data.decode("utf-8")
        json_data = json.loads(data_str)
        json_data = [
            json.loads(doc.decode("utf-8"))
            for doc in json_data
            if isinstance(doc, bytes)
        ]
        return json_data

    def load_data(self, data):
        try:
            cleaned_data = self.clean_data(data)
            for batch in cleaned_data:
                self.logger.info("Batch size: " + str(len(batch)))
                for doc in batch:
                    self.logger.info(doc)
                    self.conn.sql(
                        f"""INSERT INTO NewsData (description, source, pubDate, title)
                        VALUES (
                        '{doc["description"]}',
                        '{doc["source"]}',
                        '{doc["pubDate"]}',
                        '{doc["title"]}'
                    )
                        """
                    )
        except Exception as e:
            self.logger.error(f"Error loading News data: {e}")
