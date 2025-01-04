import pandas as pd
import plotly.express as px
import streamlit as st

from dashboard.src.core.logging_config import setup_logging
from dashboard.src.utils.mohterduck import fetch_all, init_conn

st.set_page_config(page_title="Bitcoin Dashboard", page_icon="📈", layout="wide")
logger = setup_logging("main")
st.title("Bitcoin Dashboard")
st.markdown(
    "This application is a Streamlit dashboard that can be used to analyze the price of Bitcoin"
)

df = fetch_all(init_conn())
df["market_cap"] = df["market_cap"].astype(float)
df["last_updated"] = pd.to_datetime(df["last_updated"])
df["price"] = df["price"].astype(float)


col1, col2 = st.columns([1, 2])
with col1:
    st.dataframe(df)

with col2:
    p_fig = px.line(df, x="last_updated", y="price", title="Bitcoin Price Over Time")
    p_fig.update_xaxes(tickangle=45)
    st.plotly_chart(p_fig)


# Display the chart in Streamlit
aggregated = df.groupby("last_updated").agg({"market_cap": "mean"}).reset_index()
mc_fig = px.line(
    aggregated, x="last_updated", y="market_cap", title="Bitcoin Market Cap Over Time"
)
mc_fig.update_xaxes(tickangle=45)
st.plotly_chart(mc_fig)
# st.line_chart(data=aggregated, x="last_updated", y="market_cap",x_label="Date",y_label="Market Cap")
