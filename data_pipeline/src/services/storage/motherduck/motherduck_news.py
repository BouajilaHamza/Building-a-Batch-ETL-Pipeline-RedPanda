import json

from data_pipeline.src.core.logging_config import setup_logging
from data_pipeline.src.schemas.news_schemas import NewsData
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

    def clean_data(self, batch: list) -> NewsData:
        clean_list = []
        for message in batch:
            if message:
                data_str = message["Value"].decode("utf-8")
                json_data = json.loads(data_str)
                json_data = [
                    json.loads(doc.decode("utf-8"))
                    for doc in json_data
                    if isinstance(doc, bytes)
                ]
                clean_list.extend(json_data)
        cleaned_data = NewsData(root=clean_list)
        return cleaned_data

    def load_data(self, data: list):
        try:
            cleaned_data = self.clean_data(data)
            for doc in cleaned_data.root:
                self.logger.info(doc)
                self.conn.execute(
                    "INSERT INTO NewsData VALUES (?, ?, ?, ?);",
                    [doc.description, doc.source, doc.pubDate, doc.title],
                )
        except Exception as e:
            self.logger.error(f"Error loading News data: {e}")
            raise Exception(f"Error loading News data: {e}")

    def close(self):
        self.conn.close()
