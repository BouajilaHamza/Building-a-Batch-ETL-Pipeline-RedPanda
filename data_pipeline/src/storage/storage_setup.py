from data_pipeline.storage.motherduck import MotherduckLoader
from data_pipeline.storage.snowflake import SnowflakeLoader


class NewsDataLoader(MotherduckLoader):
    pass


class BitcoinDataLoader(SnowflakeLoader):
    pass


class EthereumDataLoader(SnowflakeLoader):
    pass
