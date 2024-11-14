import unittest
from data_ingestion.news_data_ingestor import NewsDataIngestor

class TestNewsDataIngestor(unittest.TestCase):
    def test_fetch_data(self):
        ingestor = NewsDataIngestor()
        data = ingestor.fetch_data()
        self.assertTrue("results" in data)

if __name__ == '__main__':
    unittest.main()
