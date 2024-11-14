import requests 
from data_ingestor import DataIngestor
class NewsDataIngestor(DataIngestor):
    def fetch_data(self):
        self.logger.info("Fetching data from NewsData API")
        url = "https://newsdata.io/api/1/news?apikey=YOUR_API_KEY"
        response = requests.get(url)
        self.logger.info("Data fetched successfully")
        return response.json()
