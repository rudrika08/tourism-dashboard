import streamlit as st
import pandas as pd
import psycopg2

# ---------------------------
# DB CONNECTION
# ---------------------------
conn = psycopg2.connect(
    dbname="tourism_db",
    user="postgres",
    password="postgres",  # change if needed
    host="localhost",
    port="5432"
)

# ---------------------------
# LOAD DATA
# ---------------------------
@st.cache_data
def load_data():
    query = "SELECT * FROM fact_bookings"
    df = pd.read_sql(query, conn)

    # ✅ FIX: convert to datetime
    df["booking_date"] = pd.to_datetime(df["booking_date"])

    # convert numeric columns
    df["total_price_inr"] = pd.to_numeric(df["total_price_inr"], errors="coerce")
    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

    return df

df = load_data()

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
