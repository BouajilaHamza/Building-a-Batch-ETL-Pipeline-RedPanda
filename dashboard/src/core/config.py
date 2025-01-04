from decouple import config
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    MOTHERDUCK_TOKEN: str = config("MOTHERDUCK_TOKEN", cast=str)
    DATABASE_NAME: str = config("DATABASE_NAME", cast=str)
    PAPERTRAIL_HOST: str = config("PAPERTRAIL_HOST", cast=str)
    PAPERTRAIL_PORT: int = config("PAPERTRAIL_PORT", cast=int)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"


settings = Settings()
