from data_ingestor import DataIngestor
import requests


class CoinMarketCapIngestor(DataIngestor):
    def fetch_data(self):
        url = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"
        headers = {"X-CMC_PRO_API_KEY": "YOUR_API_KEY"}
        response = requests.get(url, headers=headers)
        return response.json()