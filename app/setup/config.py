from decouple import config
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # APIS
    NEWS_API_KEY: str = config("NEWS_API_KEY", cast=str)
    COIN_MARKET_CAP_API_KEY: str = config("COIN_MARKET_CAP_API_KEY", cast=str)

    # LOGGING
    PAPERTRAIL_HOST: str = config("PAPERTRAIL_HOST", cast=str)
    PAPERTRAIL_PORT: int = config("PAPERTRAIL_PORT", cast=int)

    # RedPanda
    REDPANDA_TOPIC: str = config("REDPANDA_TOPIC", cast=str)

    # Snowflake
    SNOWFLAKE_USERNAME: str = config("SNOWFLAKE_USERNAME", cast=str)
    SNOWFLAKE_PASSWORD: str = config("SNOWFLAKE_PASSWORD", cast=str)
    SNOWFLAKE_ACCOUNT: str = config("SNOWFLAKE_ACCOUNT", cast=str)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
