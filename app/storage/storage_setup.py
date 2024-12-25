from app.storage.motherduck import MotherduckLoader
from app.storage.snowflake import SnowflakeLoader


class NewsDataLoader(MotherduckLoader):
    pass


class BitcoinDataLoader(SnowflakeLoader):
    pass


class EthereumDataLoader(SnowflakeLoader):
    pass
