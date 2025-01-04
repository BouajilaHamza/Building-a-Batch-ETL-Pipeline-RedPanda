import duckdb

from dashboard.src.core.config import settings


def init_conn():
    md_conn = duckdb.connect(
        f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
    )
    md_conn.sql("USE main")
    return md_conn


def fetch_all(conn: duckdb.DuckDBPyConnection):
    query = """
    SELECT * FROM BitcoinData
    """
    return conn.execute(query).df()
