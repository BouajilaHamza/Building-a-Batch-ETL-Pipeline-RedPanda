from requests import Session
from requests.exceptions import ConnectionError, Timeout, TooManyRedirects

from app.data_ingestion.data_ingestor import DataIngestor
from app.config.setup import settings


class CoinMarketCapIngestor(DataIngestor):
    def __init__(self, start=1, limit=5000, convert='USD'):
        super().__init__()
        self.headers = {
            'Accepts': 'application/json',
            'X-CMC_PRO_API_KEY': settings.COIN_MARKET_CAP_API_KEY,
        }
        self.session = Session()
        self.session.headers.update(self.headers)
        self.url = 'https://sandbox-api.coinmarketcap.com/v1/cryptocurrency/listings/latest'
        self.parameters = {
            'start': start,
            'limit': limit,
            'convert': convert
        }

    def fetch_data(self):
        try:
            self.logger.info("Fetching data from CoinMarketCap API")
            response = self.session.get(self.url, params=self.parameters)
            response.raise_for_status()  # Check if the request was successful
            data = response.json()  # Directly parse JSON response
            self.logger.info(f"Data fetched successfully: {response}")
            return data
        except (ConnectionError, Timeout, TooManyRedirects, Exception) as e:
            self.logger.error(f"An error occurred: {e}")
            return None
