import streamlit as st
import pandas as pd
import os
from pathlib import Path
import psycopg2
import plotly.express as px


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
# UI tweaks: KPI card styles
st.markdown(
    """
    <style>
    .kpi {background-color:#0f1720;padding:12px;border-radius:8px;color:#ffffff}
    .metric {font-size:20px;}
    </style>
    """,
    unsafe_allow_html=True,
)
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

# Chart controls
dest_chart_type = st.sidebar.selectbox("Destinations chart type", ["Bar", "Pie"], index=0)
sort_desc = st.sidebar.checkbox("Sort destinations descending", value=True)

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
total_bookings = len(filtered_df)
total_revenue = filtered_df[revenue_column].sum() if revenue_column in filtered_df.columns else 0
avg_rating = filtered_df["rating"].mean() if "rating" in filtered_df.columns else None

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"<div class='kpi'><div class='metric'>📦<strong> Total Bookings</strong></div><div style='font-size:24px'>{total_bookings:,}</div></div>", unsafe_allow_html=True)

with col2:
    st.markdown(f"<div class='kpi'><div class='metric'>💰<strong> Total Revenue</strong></div><div style='font-size:24px'>₹ {int(total_revenue):,}</div></div>", unsafe_allow_html=True)

with col3:
    avg_rating_display = f"{avg_rating:.2f}" if avg_rating is not None else "—"
    st.markdown(f"<div class='kpi'><div class='metric'>⭐<strong> Avg Rating</strong></div><div style='font-size:24px'>{avg_rating_display}</div></div>", unsafe_allow_html=True)

# ---------------------------
# 📍 TOP DESTINATIONS
# ---------------------------
st.subheader("📍 Top Destinations")
dest = filtered_df.groupby("destination_id").size().reset_index(name="count")

# apply sort
dest = dest.sort_values("count", ascending=not sort_desc)

if dest_chart_type == "Bar":
    fig_dest = px.bar(dest, x="destination_id", y="count", color="count", labels={"destination_id":"Destination","count":"Bookings"}, title="Top Destinations")
    fig_dest.update_traces(hovertemplate='Destination: %{x}<br>Bookings: %{y}<extra></extra>')
    st.plotly_chart(fig_dest, use_container_width=True)
else:
    fig_pie = px.pie(dest, names="destination_id", values="count", title="Bookings by Destination", hole=0.3)
    fig_pie.update_traces(hovertemplate="%{label}: %{value} bookings<extra></extra>")
    st.plotly_chart(fig_pie, use_container_width=True)

# ---------------------------
# 🧑‍🏫 GUIDE PERFORMANCE
# ---------------------------
st.subheader("🧑‍🏫 Guide Performance")

guide_perf = (
    filtered_df.groupby("guide_id")["rating"].mean().reset_index().sort_values("rating", ascending=False)
)

fig_guides = px.bar(guide_perf, x="guide_id", y="rating", color="rating", labels={"guide_id":"Guide","rating":"Avg Rating"}, title="Guide Average Ratings")
fig_guides.update_traces(hovertemplate='Guide: %{x}<br>Avg Rating: %{y:.2f}<extra></extra>')
st.plotly_chart(fig_guides, use_container_width=True)

# ---------------------------
# 📅 BOOKING TRENDS
# ---------------------------
st.subheader("📅 Booking Trends")

if "booking_date" in filtered_df.columns:
    trend = filtered_df.groupby(filtered_df["booking_date"].dt.date).size().reset_index(name="count")
    fig_trend = px.line(trend, x="booking_date", y="count", labels={"booking_date":"Date","count":"Bookings"}, title="Bookings Over Time")
    fig_trend.update_traces(mode='lines+markers', hovertemplate='Date: %{x}<br>Bookings: %{y}<extra></extra>')
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("No booking_date column available for trends.")

# ---------------------------
# 💡 REVENUE BY TOUR TYPE
# ---------------------------
st.subheader("💡 Revenue by Tour Type")

if "tour_type" in filtered_df.columns:
    tour = filtered_df.groupby("tour_type")[revenue_column].sum().reset_index().sort_values(revenue_column, ascending=False)
    fig_tour = px.bar(tour, x="tour_type", y=revenue_column, labels={"tour_type":"Tour Type", revenue_column:"Revenue"}, title="Revenue by Tour Type")
    fig_tour.update_traces(hovertemplate='Tour: %{x}<br>Revenue: ₹%{y:,.0f}<extra></extra>')
    st.plotly_chart(fig_tour, use_container_width=True)
else:
    st.info("No tour_type column available.")

# ---------------------------
# 🧾 RAW DATA (OPTIONAL)
# ---------------------------
st.subheader("🧾 View Data")

if st.checkbox("Show Raw Data"):
    st.dataframe(filtered_df)

    csv = filtered_df.to_csv(index=False)
    st.download_button("Download filtered data (CSV)", csv, file_name="filtered_data.csv", mime="text/csv")
