import streamlit as st
import pandas as pd
import os
from pathlib import Path
import psycopg2


DATA_PATH = Path(__file__).resolve().parents[1] / "data" / "final_analytics_dataset.csv"

# ---------------------------
# LOAD DATA
# ---------------------------
@st.cache_data
def load_data():
    database_url = os.getenv("DATABASE_URL")
    db_sslmode = os.getenv("DB_SSLMODE", "require")
    conn = None

    if database_url:
        try:
            conn = psycopg2.connect(database_url, sslmode=db_sslmode)

            for query in ("SELECT * FROM fact_bookings", "SELECT * FROM fact_bookings_stream"):
                try:
                    df = pd.read_sql(query, conn)
                    break
                except Exception:
                    df = None
            else:
                df = None

            if df is not None and not df.empty:
                if "booking_date" in df.columns:
                    df["booking_date"] = pd.to_datetime(df["booking_date"], errors="coerce")

                if "price" in df.columns:
                    df["price"] = pd.to_numeric(df["price"], errors="coerce")

                if "total_price_inr" in df.columns:
                    df["total_price_inr"] = pd.to_numeric(df["total_price_inr"], errors="coerce")

                if "rating" in df.columns:
                    df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

                return df
        except Exception as exc:
            st.warning(f"Database unavailable, falling back to bundled dataset: {exc}")
        finally:
            if conn is not None:
                conn.close()

    if DATA_PATH.exists():
        df = pd.read_csv(DATA_PATH)

        if "booking_date" in df.columns:
            df["booking_date"] = pd.to_datetime(df["booking_date"], errors="coerce")

        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce")

        if "total_price_inr" in df.columns:
            df["total_price_inr"] = pd.to_numeric(df["total_price_inr"], errors="coerce")

        if "rating" in df.columns:
            df["rating"] = pd.to_numeric(df["rating"], errors="coerce")

        return df

    st.error("No database connection was available and the bundled dataset could not be found.")
    return pd.DataFrame()

df = load_data()

if df.empty:
    st.stop()

revenue_column = "price" if "price" in df.columns else "total_price_inr"

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
    f"₹ {round(filtered_df[revenue_column].sum(), 2)}"
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
    filtered_df.groupby("tour_type")[revenue_column]
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
