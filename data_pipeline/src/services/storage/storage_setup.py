from data_pipeline.src.services.storage.motherduck.motherduck_init import (
    MotherduckLoader,
)
from data_pipeline.src.services.storage.snowflake.snowflake import SnowflakeLoader


class NewsDataLoader(MotherduckLoader):
    pass


class BitcoinDataLoader(SnowflakeLoader):
    pass


class EthereumDataLoader(SnowflakeLoader):
    pass
