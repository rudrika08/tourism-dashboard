import streamlit as st
import pandas as pd
import os
import psycopg2

# ---------------------------
# LOAD DATA
# ---------------------------
@st.cache_data
def load_data():
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        st.error("DATABASE_URL is not set. Configure the Railway connection string in your environment.")
        return pd.DataFrame()

    conn = None
    try:
        conn = psycopg2.connect(database_url, sslmode="require")

        for query in ("SELECT * FROM fact_bookings", "SELECT * FROM fact_bookings_stream"):
            try:
                df = pd.read_sql(query, conn)
                break
            except Exception:
                df = None
        else:
            return pd.DataFrame()

        if df is None:
            return pd.DataFrame()

        if "booking_date" in df.columns:
            df["booking_date"] = pd.to_datetime(df["booking_date"], errors="coerce")

        if "total_price_inr" in df.columns:
            df["total_price_inr"] = pd.to_numeric(df["total_price_inr"], errors="coerce")

        if "rating" in df.columns:
            df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        return df
    except Exception as exc:
        st.error(f"Failed to load data: {exc}")
        return pd.DataFrame()
    finally:
        if conn is not None:
            conn.close()

df = load_data()

if df.empty:
    st.stop()

# ---------------------------
# TITLE
# ---------------------------
st.title("📊 Tourism Analytics Dashboard")

# ---------------------------
# 🎯 FILTERS
# ---------------------------
st.sidebar.header("🔍 Filters")

destination_filter = st.sidebar.multiselect(
    "Select Destination",
    options=df["destination_id"].dropna().unique(),
    default=df["destination_id"].dropna().unique()
)

guide_filter = st.sidebar.multiselect(
    "Select Guide",
    options=df["guide_id"].dropna().unique(),
    default=df["guide_id"].dropna().unique()
)

date_range = st.sidebar.date_input(
    "Select Date Range",
    [df["booking_date"].min(), df["booking_date"].max()]
)

# Ensure valid range
start_date = pd.to_datetime(date_range[0])
end_date = pd.to_datetime(date_range[1])

# ---------------------------
# APPLY FILTERS
# ---------------------------
filtered_df = df[
    (df["destination_id"].isin(destination_filter)) &
    (df["guide_id"].isin(guide_filter)) &
    (df["booking_date"].between(start_date, end_date))
]

# ---------------------------
# 📌 KPIs
# ---------------------------
st.subheader("📌 Key Metrics")

col1, col2, col3 = st.columns(3)

col1.metric("Total Bookings", len(filtered_df))

col2.metric(
    "Total Revenue",
    f"₹ {round(filtered_df['total_price_inr'].sum(), 2)}"
)

col3.metric(
    "Avg Rating",
    round(filtered_df["rating"].mean(), 2)
)

# ---------------------------
# 📍 TOP DESTINATIONS
# ---------------------------
st.subheader("📍 Top Destinations")

dest = (
    filtered_df.groupby("destination_id")
    .size()
    .sort_values(ascending=False)
)

st.bar_chart(dest)

# ---------------------------
# 🧑‍🏫 GUIDE PERFORMANCE
# ---------------------------
st.subheader("🧑‍🏫 Guide Performance")

guide_perf = (
    filtered_df.groupby("guide_id")["rating"]
    .mean()
    .sort_values(ascending=False)
)

st.bar_chart(guide_perf)

# ---------------------------
# 📅 BOOKING TRENDS
# ---------------------------
st.subheader("📅 Booking Trends")

trend = (
    filtered_df.groupby("booking_date")
    .size()
)

st.line_chart(trend)

# ---------------------------
# 💡 REVENUE BY TOUR TYPE
# ---------------------------
st.subheader("💡 Revenue by Tour Type")

tour = (
    filtered_df.groupby("tour_type")["total_price_inr"]
    .sum()
    .sort_values(ascending=False)
)

st.bar_chart(tour)

# ---------------------------
# 🧾 RAW DATA (OPTIONAL)
# ---------------------------
st.subheader("🧾 View Data")

if st.checkbox("Show Raw Data"):
    st.dataframe(filtered_df)
