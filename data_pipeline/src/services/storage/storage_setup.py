from data_pipeline.src.services.storage.motherduck import MotherduckLoader
from data_pipeline.src.services.storage.snowflake import SnowflakeLoader


class NewsDataLoader(MotherduckLoader):
    pass


class BitcoinDataLoader(SnowflakeLoader):
    pass


class EthereumDataLoader(SnowflakeLoader):
    pass
