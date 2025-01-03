import json

from data_pipeline.src.services.storage.motherduck.motherduck_init import (
    MotherduckLoader,
)


class NewsDataLoader(MotherduckLoader):
    def __init__(self):
        super().__init__()
        self.create_table_if_not_exists()

    def create_table_if_not_exists(self):
        self.conn.sql("""
            CREATE TABLE IF NOT EXISTS NewsData (
                description VARCHAR(255),
                source VARCHAR(255),
                pubDate VARCHAR(255),
                title VARCHAR(255),
            );
        """)

    def clean_data(self, raw_data):
        if isinstance(raw_data, list):
            return raw_data
        data_str = raw_data.decode("utf-8")
        json_data = json.loads(data_str)
        return json_data

    def load_data(self, data):
        cleaned_data = self.clean_data(data)
        self.conn.sql(
            f"""INSERT INTO NewsData (description, source, pubDate, title)
            VALUES (
            '{cleaned_data["description"]}',
            '{cleaned_data["source"]}',
            '{cleaned_data["pubDate"]}',
            '{cleaned_data["title"]}',
        )
            """
        )
