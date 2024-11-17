from decouple import config
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    NEWS_API_KEY: str = config("NEWS_API_KEY",cast=str)

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

settings = Settings()