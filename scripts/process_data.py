import pandas as pd
from datetime import datetime

data_path = "../data/"
log_path = "../logs/pipeline.log"

def log(msg):
    with open(log_path, "a") as f:
        f.write(f"{datetime.now()} - {msg}\n")

try:
    log("Pipeline Started")

    # Load datasets
    bookings = pd.read_csv(data_path + "tourist_bookings.csv")
    tourists = pd.read_csv(data_path + "tourists_dimension.csv")
    guides = pd.read_csv(data_path + "guides_dimension.csv")
    destinations = pd.read_csv(data_path + "destinations_dimension.csv")
    dates = pd.read_csv(data_path + "date_dimension.csv")

    log("All datasets loaded")

    # 🔧 Handle NULL values properly
    bookings.replace("NULL", pd.NA, inplace=True)

    # Convert date columns to datetime
    bookings["booking_date"] = pd.to_datetime(bookings["booking_date"])
    dates["full_date"] = pd.to_datetime(dates["full_date"])

    # Basic cleaning
    bookings = bookings.drop_duplicates()
    tourists = tourists.drop_duplicates()
    guides = guides.drop_duplicates()
    destinations = destinations.drop_duplicates()
    dates = dates.drop_duplicates()

    log("Duplicates removed and NULLs handled")

    # 🔗 Merge step-by-step
    df = bookings.merge(tourists, on="tourist_id", how="left")
    df = df.merge(guides, on="guide_id", how="left")
    df = df.merge(destinations, on="destination_id", how="left")

    # ✅ FIXED DATE JOIN
    df = df.merge(dates, left_on="booking_date", right_on="full_date", how="left")

    log("Data merged successfully")

    # 🧠 Feature Engineering
    if "total_price_inr" in df.columns:
        df["revenue_per_person"] = df["total_price_inr"] / df["num_tourists"]

    # 📊 Basic Insights
    print("\n📊 Basic Insights:")

    if "destination_id" in df.columns:
        print("Top Destination:", df["destination_id"].value_counts().idxmax())

    if "guide_id" in df.columns:
        print("Total Guides Used:", df["guide_id"].nunique())

    if "total_price_inr" in df.columns:
        print("Total Revenue:", df["total_price_inr"].sum())

    # 💾 Save final dataset
    df.to_csv(data_path + "final_analytics_dataset.csv", index=False)

    log("Final dataset saved")
    print("✅ Pipeline executed successfully")

except Exception as e:
    log(str(e))
    print("❌ Error:", e)
