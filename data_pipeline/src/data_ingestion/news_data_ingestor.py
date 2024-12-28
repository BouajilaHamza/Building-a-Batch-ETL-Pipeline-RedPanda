import requests

from data_pipeline.data_ingestion.data_ingestor import DataIngestor
from data_pipeline.setup.config import settings


class NewsDataIngestor(DataIngestor):
    def __init__(self, category="politics", country="bd"):
        super().__init__()
        self.category = category
        self.country = country

    def fetch_data(self):
        try:
            self.logger.info("Fetching data from NewsData API")
            url = f"https://newsdata.io/api/1/latest?apikey={settings.NEWS_API_KEY}&category={self.category}&country={self.country}"
            response = requests.get(url)
            self.logger.info(f"Data fetched successfully from NewsData API {response}")
            return response.json()
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            return None
