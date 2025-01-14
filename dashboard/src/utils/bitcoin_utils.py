import duckdb
from dashboard.src.core.config import settings

def __init__(self):
    self.conn = duckdb.connect(
        f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
    )
    self.conn.sql("USE main")


def fetch_all(conn: duckdb.DuckDBPyConnection):
    query = """
    SELECT price,market_cap, last_updated FROM BitcoinData
    """
    return conn.execute(query).df()

