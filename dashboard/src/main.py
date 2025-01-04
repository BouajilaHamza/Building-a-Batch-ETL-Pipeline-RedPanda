import streamlit as st
from src.core.logging_config import setup_logging
from src.utils.mohterduck import fetch_all, init_conn

st.set_page_config(page_title="Bitcoin Dashboard", page_icon="📈", layout="wide")
logger = setup_logging("main")
st.title("Bitcoin Dashboard")
st.markdown(
    "This application is a Streamlit dashboard that can be used to analyze the price of Bitcoin"
)

col1, col2 = st.columns([4, 2])
with col1:
    df = fetch_all(init_conn())
    st.dataframe(df)

with col2:
    st.line_chart(data=df, x="last_updated", y="price")
    df["price"] = df["price"].astype(float)

    aggregated = df.groupby("last_updated").agg({"price": "mean"}).reset_index()
    st.bar_chart(data=aggregated, x="last_updated", y="price")
