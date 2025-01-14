import pandas as pd
import plotly.express as px
import streamlit as st
from streamlit_autorefresh import st_autorefresh

from dashboard.src.core.logging_config import setup_logging
from dashboard.src.utils.mohterduck import init_conn
from dashboard.src.utils.bitcoin_utils import fetch_all
from dashboard.src.utils.news_utils import fetch_all_news
st.set_page_config(page_title="Motherduck Insights Dashboard", page_icon="📈", layout="wide")


count = st_autorefresh(interval=30 * 1000, key="data_refresh")

logger = setup_logging("main")
st.title("Bitcoin Dashboard")
st.markdown(
    "This application is a Streamlit dashboard that can be used to analyze the price of Bitcoin"
)

bitcoin_data = fetch_all(init_conn())
news_data = fetch_all_news(init_conn())


bitcoin_data["market_cap"] = bitcoin_data["market_cap"].astype(float)
bitcoin_data["last_updated"] = pd.to_datetime(bitcoin_data["last_updated"])
bitcoin_data["price"] = bitcoin_data["price"].astype(float)


col1, col2 = st.columns([1, 2])
with col1:
    st.dataframe(bitcoin_data)

with col2:
    p_fig = px.line(bitcoin_data, x="last_updated", y="price", title="Bitcoin Price Over Time")
    p_fig.update_xaxes(tickangle=45)
    st.plotly_chart(p_fig)


# Display the chart in Streamlit
aggregated = bitcoin_data.groupby("last_updated").agg({"market_cap": "mean"}).reset_index()
mc_fig = px.line(
    aggregated, x="last_updated", y="market_cap", title="Bitcoin Market Cap Over Time"
)
mc_fig.update_xaxes(tickangle=45)
st.plotly_chart(mc_fig)
# st.line_chart(data=aggregated, x="last_updated", y="market_cap",x_label="Date",y_label="Market Cap")
col3, col4 = st.columns([1, 1])
with col3:
    st.metric("Unique Price Values", len(bitcoin_data.price.unique()))




st.header("Bitcoin Prices")
col4 , col5  = st.columns([1,1])
# Title of the app
with col4:
    # Display basic statistics
    st.subheader("Basic Statistics")
    st.write(bitcoin_data.describe())
with col5:
    # Check for missing values
    st.subheader("Missing Values")
    st.write(bitcoin_data.isnull().sum())


st.header("News Data")
col6,col7,col8 = st.columns([1,1,1])
    # Section for News Data
with col6:
    st.write("Inspect the news data:")
    st.dataframe(news_data)

with col7:
    # # Display basic statistics for news data
    st.subheader("Basic Statistics for News")
    st.write(news_data.describe())

with col8:
    # # Check for missing values in news data
    st.subheader("Missing Values in News Data")
    st.write(news_data.isnull().sum())

# Debug info
st.write(f"Refreshed {count} times")
