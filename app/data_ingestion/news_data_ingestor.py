import requests 
from app.data_ingestion.data_ingestor import DataIngestor
from app.config.setup import settings

class NewsDataIngestor(DataIngestor):
    def fetch_data(self):
        self.logger.info("Fetching data from NewsData API")

        url = f"https://newsdata.io/api/1/latest?apikey={settings.NEWS_API_KEY}&category=politics&country=bd"
        response = requests.get(url)
        self.logger.info("Data fetched successfully")
        return response.json()



news_ingestor = NewsDataIngestor()

result = news_ingestor.fetch_data()
print(result)
