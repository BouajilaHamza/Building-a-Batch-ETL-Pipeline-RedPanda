import duckdb
from dashboard.src.core.config import settings

def __init__(self):
    self.conn = duckdb.connect(
        f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
    )
    self.conn.sql("USE main")


def fetch_all_news(conn: duckdb.DuckDBPyConnection):
    query = """
    FROM NewsData SELECT *
    """
    return conn.execute(query).df()
