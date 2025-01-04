import logging
from logging.handlers import SysLogHandler

from src.core.config import settings


def setup_logging(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    handler = SysLogHandler(
        address=(settings.PAPERTRAIL_HOST, settings.PAPERTRAIL_PORT)
    )
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)

    logging.basicConfig(level=logging.INFO, handlers=[handler])

    return logger
