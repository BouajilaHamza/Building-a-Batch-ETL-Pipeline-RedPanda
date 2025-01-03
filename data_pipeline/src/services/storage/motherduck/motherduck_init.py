import duckdb

from data_pipeline.src.core.config import settings
from data_pipeline.src.core.logging_config import setup_logging


class MotherduckLoader:
    def __init__(self):
        self.logger = setup_logging("MotherduckLoader")
        self.conn = self.connect_to_database()

    def connect_to_database(self):
        try:
            # First try to connect to check if database exists
            conn = duckdb.connect(
                f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
            )
            self.logger.info(
                f"Successfully connected to existing database {settings.DATABASE_NAME}"
            )
            return conn
        except Exception as e:
            if "no database/share named" in str(e):
                self.logger.info(
                    f"Database {settings.DATABASE_NAME} not found. Creating new database..."
                )
                # Connect to MotherDuck without specifying database
                temp_conn = duckdb.connect(
                    f"md:?motherduck_token={settings.MOTHERDUCK_TOKEN}"
                )
                # Create the database
                temp_conn.sql(f"CREATE DATABASE IF NOT EXISTS {settings.DATABASE_NAME}")
                temp_conn.close()
                # Now connect to the newly created database
                conn = duckdb.connect(
                    f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
                )
                self.logger.info(
                    f"Successfully created and connected to database {settings.DATABASE_NAME}"
                )
                return conn
            else:
                self.logger.error(f"Failed to connect to MotherDuck: {str(e)}")
                raise

    def clean_data(self, raw_data: bytes | list) -> list:
        raise NotImplementedError("This method should be overridden by subclasses")

    def load_data(self, data: bytes):
        raise NotImplementedError("This method should be overridden by subclasses")

    def close_connection(self):
        if hasattr(self, "conn") and self.conn:
            self.conn.close()
            self.logger.info("Connection closed successfully")
