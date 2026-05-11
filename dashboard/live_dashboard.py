import streamlit as st
import pandas as pd
import os
from pathlib import Path
import psycopg2
import plotly.express as px
from streamlit_autorefresh import st_autorefresh


STREAMING_SAMPLE_PATH = Path(__file__).resolve().parents[1] / "data" / "streaming_events.csv"

st.set_page_config(
    page_title="Tourism Analytics",
    page_icon="🌍",
    layout="wide"
)

# 🔄 Auto refresh every 5 seconds
st_autorefresh(interval=5000, key="datarefresh")

# 🎨 Custom UI
st.markdown("""
    <style>
    .main {background-color: #0E1117;}
    .stMetric {background                    -color: #1c1f26; padding: 15px; border-radius: 10px;}
    </style>
""", unsafe_allow_html=True)

st.title("🌍 Tourism Real-Time Analytics Dashboard")

@st.cache_data(ttl=5)
def load_data():
    database_url = os.getenv("DATABASE_URL")
    db_sslmode = os.getenv("DB_SSLMODE", "require")
    conn = None

    if database_url:
        try:
            conn = psycopg2.connect(database_url, sslmode=db_sslmode)

            query = "SELECT * FROM fact_bookings_stream"
            df = pd.read_sql(query, conn)
            return df
        except Exception as exc:
            st.warning(f"Database unavailable, falling back to bundled streaming sample: {exc}")
        finally:
            try:
                if conn is not None:
                    conn.close()
            except Exception:
                pass

    if STREAMING_SAMPLE_PATH.exists():
        df = pd.read_csv(STREAMING_SAMPLE_PATH)
        if "amount_inr" in df.columns and "price" not in df.columns:
            df["price"] = pd.to_numeric(df["amount_inr"], errors="coerce")
        return df

    st.error("No database connection was available and the bundled streaming sample could not be found.")
    return pd.DataFrame()

df = load_data()
# 📥 Load data

if not df.empty:
    revenue_column = "price" if "price" in df.columns else "total_price_inr"

    # 🎯 SIDEBAR FILTERS
    st.sidebar.header("🔍 Filters")

    destinations = st.sidebar.multiselect(
        "Destination",
        df["destination_id"].unique(),
        default=df["destination_id"].unique(),
        key="destination_filter"
    )

    price_range = st.sidebar.slider(
        "Price Range",
        int(df[revenue_column].min()),
        int(df[revenue_column].max()),
        (int(df[revenue_column].min()), int(df[revenue_column].max())),
        key="price_filter"
    )

    # 🧹 Apply filters
    filtered_df = df[
        (df["destination_id"].isin(destinations)) &
        (df[revenue_column].between(price_range[0], price_range[1]))
    ]

    # 📊 KPIs
    st.subheader("📊 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric("📦 Total Records", len(filtered_df))
    col2.metric("💰 Revenue", f"₹ {int(filtered_df[revenue_column].sum())}")
    col3.metric("📊 Avg Price", f"₹ {int(filtered_df[revenue_column].mean())}")
    col4.metric("🌍 Unique Destinations", filtered_df["destination_id"].nunique())

    st.divider()

    # 📈 Charts Row 1
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("💰 Revenue by Destination")
        fig1 = px.bar(
            filtered_df.groupby("destination_id")[revenue_column].sum().reset_index(),
            x="destination_id",
            y=revenue_column,
            color="destination_id"
        )
        st.plotly_chart(fig1, use_container_width=True)

    with col6:
        st.subheader("📦 Booking Distribution")
        fig2 = px.pie(
            filtered_df,
            names="destination_id"
        )
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # 📉 Charts Row 2
    col7, col8 = st.columns(2)

    with col7:
        st.subheader("📊 Price Distribution")
        fig3 = px.histogram(filtered_df, x=revenue_column, nbins=20)
        st.plotly_chart(fig3, use_container_width=True)

    st.divider()

    # 🔥 Advanced Insights
    st.subheader("🔥 Insights")

    col8, col9 = st.columns(2)

    with col8:
        high_value = filtered_df[filtered_df[revenue_column] > 5000]
        st.success(f"High Value Bookings: {len(high_value)}")

    with col9:
        low_value = filtered_df[filtered_df[revenue_column] < 3000]
        st.warning(f"Low Value Bookings: {len(low_value)}")

    st.divider()

    # 📋 Table
    st.subheader("📋 Latest Data")
    st.dataframe(filtered_df.tail(10), use_container_width=True)

else:
    st.warning("⏳ Waiting for streaming data...")

