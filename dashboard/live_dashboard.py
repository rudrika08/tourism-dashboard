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

# Small CSS for KPI cards
st.markdown(
    """
    <style>
    .kpi {background-color:#071029;padding:12px;border-radius:8px;color:#fff}
    </style>
    """,
    unsafe_allow_html=True,
)

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

    # Chart controls
    dest_chart_type = st.sidebar.selectbox("Destinations chart type", ["Bar", "Donut"], index=0)
    sort_desc = st.sidebar.checkbox("Sort destinations descending", value=True)

    # 🧹 Apply filters
    filtered_df = df[
        (df["destination_id"].isin(destinations)) &
        (df[revenue_column].between(price_range[0], price_range[1]))
    ]

    # 📊 KPIs
    total_records = len(filtered_df)
    total_revenue = int(filtered_df[revenue_column].sum()) if revenue_column in filtered_df.columns else 0
    avg_price = int(filtered_df[revenue_column].mean()) if revenue_column in filtered_df.columns else 0
    unique_dest = filtered_df["destination_id"].nunique()

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.markdown(f"<div class='kpi'>📦 <strong>Total Records</strong><div style='font-size:22px'>{total_records:,}</div></div>", unsafe_allow_html=True)
    with c2:
        st.markdown(f"<div class='kpi'>💰 <strong>Total Revenue</strong><div style='font-size:22px'>₹ {total_revenue:,}</div></div>", unsafe_allow_html=True)
    with c3:
        st.markdown(f"<div class='kpi'>📊 <strong>Avg Price</strong><div style='font-size:22px'>₹ {avg_price:,}</div></div>", unsafe_allow_html=True)
    with c4:
        st.markdown(f"<div class='kpi'>🌍 <strong>Unique Destinations</strong><div style='font-size:22px'>{unique_dest}</div></div>", unsafe_allow_html=True)

    st.divider()

    # 📈 Charts Row 1
    col5, col6 = st.columns(2)

    with col5:
        st.subheader("💰 Revenue by Destination")
        dest_rev = filtered_df.groupby("destination_id")[revenue_column].sum().reset_index()
        dest_rev = dest_rev.sort_values(revenue_column, ascending=not sort_desc)
        if dest_chart_type == "Bar":
            fig1 = px.bar(dest_rev, x="destination_id", y=revenue_column, color=revenue_column, labels={"destination_id":"Destination"}, title="Revenue by Destination")
            fig1.update_traces(hovertemplate='Destination: %{x}<br>Revenue: ₹%{y:,.0f}<extra></extra>')
            st.plotly_chart(fig1, use_container_width=True)
        else:
            fig1 = px.pie(dest_rev, names="destination_id", values=revenue_column, title="Revenue by Destination", hole=0.4)
            fig1.update_traces(hovertemplate='%{label}: ₹%{value:,.0f}<extra></extra>')
            st.plotly_chart(fig1, use_container_width=True)

    with col6:
        st.subheader("📦 Booking Distribution")
        dist = filtered_df["destination_id"].value_counts().reset_index()
        dist.columns = ["destination_id","count"]
        fig2 = px.pie(dist, names="destination_id", values="count", title="Bookings by Destination", hole=0.4)
        st.plotly_chart(fig2, use_container_width=True)

    st.divider()

    # 📉 Charts Row 2
    col7, col8 = st.columns(2)

    with col7:
        st.subheader("📊 Price Distribution")
        fig3 = px.histogram(filtered_df, x=revenue_column, nbins=20, labels={revenue_column:"Price (INR)"})
        fig3.update_traces(hovertemplate='Price: %{x}<br>Count: %{y}<extra></extra>')
        st.plotly_chart(fig3, use_container_width=True)

    # Add time series trends if date available
    if "booking_date" in filtered_df.columns:
        ts = filtered_df.groupby(filtered_df["booking_date"].dt.date).size().reset_index(name="count")
        fig_ts = px.line(ts, x="booking_date", y="count", title="Bookings Over Time", labels={"booking_date":"Date","count":"Bookings"})
        fig_ts.update_traces(mode='lines+markers', hovertemplate='Date: %{x}<br>Bookings: %{y}<extra></extra>')
        st.plotly_chart(fig_ts, use_container_width=True)

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
    csv = filtered_df.to_csv(index=False)
    st.download_button("Download filtered streaming data (CSV)", csv, file_name="filtered_streaming_data.csv", mime="text/csv")

else:
    st.warning("⏳ Waiting for streaming data...")

