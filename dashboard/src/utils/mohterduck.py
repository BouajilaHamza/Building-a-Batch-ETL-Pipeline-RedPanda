from datetime import timedelta

import duckdb
import streamlit as st

from dashboard.src.core.config import settings


def init_conn():
    md_conn = duckdb.connect(
        f"md:{settings.DATABASE_NAME}?motherduck_token={settings.MOTHERDUCK_TOKEN}"
    )
    md_conn.sql("USE main")
    return md_conn


@st.fragment(run_every=timedelta(minutes=1))
def fetch_all(conn: duckdb.DuckDBPyConnection):
    query = """
    SELECT price,market_cap, last_updated FROM BitcoinData
    """
    return conn.execute(query).df()
