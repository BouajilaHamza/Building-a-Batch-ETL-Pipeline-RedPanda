import logging
from logging.handlers import SysLogHandler
from app.config.setup import settings



class DataIngestor:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger("DataIngestor")
        self.logger.setLevel(logging.INFO)
        self.handler = SysLogHandler(address=(
                                              settings.PAPERTRAIL_HOST,
                                              settings.PAPERTRAIL_PORT
                                              )
                                    )
        self.logger.addHandler(self.handler)
        self.logger.info("DataIngestor initialized")

    def fetch_data(self):
        raise NotImplementedError("This method should be overridden by subclasses")
