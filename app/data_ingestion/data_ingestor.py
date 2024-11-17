import logging

class DataIngestor:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger(__name__)

    def fetch_data(self):
        raise NotImplementedError("This method should be overridden by subclasses")
