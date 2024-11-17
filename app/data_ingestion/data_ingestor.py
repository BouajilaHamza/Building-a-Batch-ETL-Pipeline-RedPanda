from app.config.logging_config import setup_logging



class DataIngestor:
    def __init__(self):
        self.logger = setup_logging("DataIngestor")
        self.logger.info("DataIngestor initialized")

    def fetch_data(self):
        raise NotImplementedError("This method should be overridden by subclasses")
