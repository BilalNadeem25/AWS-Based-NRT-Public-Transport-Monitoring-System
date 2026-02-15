import streamlit as st
import pandas as pd
from pyathena import connect
from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timezone

# =====================================
# Page setup
# =====================================
st.set_page_config(page_title="GTFS Traffic Control", layout="wide")

st.title("🚍 GTFS Real-Time Traffic Control")
st.caption("Live passenger and operations intelligence")

st.markdown("""
This dashboard monitors congestion, delays, and data freshness
using near real-time GTFS data refreshed every 30 seconds.
""")

st.markdown(
    "🟢 **Normal ≥ 30 km/h** &nbsp;&nbsp; "
    "🟡 **Slow 15–30 km/h** &nbsp;&nbsp; "
    "🔴 **Congested < 15 km/h**"
)

st_autorefresh(interval=30 * 1000, key="refresh")

# =====================================
# Thresholds
# =====================================
c1, c2, c3 = st.columns(3)
congest_th = c1.slider("Congested below (km/h)", 5, 30, 15)
stale_th = c2.slider("Stale after (seconds)", 30, 180, 60)
stop_th = c3.slider("Stopped risk after (seconds)", 30, 180, 90)

# =====================================
# Athena connection
# =====================================
@st.cache_resource
def get_athena_connection():
    return connect(
        s3_staging_dir="s3://gtfs-s3/athena_results/",
        region_name="us-east-1"
    )

conn = get_athena_connection()

# =====================================
# Data loading
# =====================================
@st.cache_data(ttl=30)
def load_vehicle_data():
    return pd.read_sql("""
        SELECT
            route_id,
            trip_id,
            vehicle_id,
            latitude,
            longitude,
            speed,
            last_update
        FROM vehicle_latest_parquet
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
    """, conn)

@st.cache_data(ttl=30)
def load_route_metrics():
    return pd.read_sql("""
        SELECT
            route_id,
            vehicle_count,
            avg_speed,
            last_update
        FROM route_metrics_parquet
    """, conn)

@st.cache_data(ttl=30)
def load_trip_metrics():
    return pd.read_sql("""
        SELECT
            trip_id,
            route_id,
            avg_speed,
            last_update
        FROM trip_metrics_parquet
    """, conn)

vehicle_df = load_vehicle_data()
route_df = load_route_metrics()
trip_df = load_trip_metrics()

# =====================================
# Time handling
# =====================================
now = datetime.now(timezone.utc)

for df in [vehicle_df, route_df, trip_df]:
    df["last_update"] = pd.to_datetime(df["last_update"], utc=True, errors="coerce")

vehicle_df["seconds_since_update"] = (
    now - vehicle_df["last_update"]
).dt.total_seconds()

# =====================================
# CORE METRICS
# =====================================
total_buses = vehicle_df["vehicle_id"].nunique()
stopped_buses = vehicle_df[vehicle_df["speed"] == 0]
pct_stopped = round(len(stopped_buses) / total_buses * 100, 1) if total_buses else 0

stale_buses = vehicle_df[vehicle_df["seconds_since_update"] > stale_th]
pct_stale = round(len(stale_buses) / total_buses * 100, 1) if total_buses else 0

congested_routes = (route_df["avg_speed"] < congest_th).sum()
total_routes = route_df["route_id"].nunique()

# =====================================
# KPI BAR
# =====================================
st.markdown("## 🚦 Live Network Status")

k1, k2, k3, k4, k5 = st.columns(5)

k1.metric("Avg Network Speed", round(route_df["avg_speed"].mean(), 1))
k2.metric("Routes Congested", f"{congested_routes}/{total_routes}")
k3.metric("Buses Stopped", f"{pct_stopped}%")
k4.metric("Stale Buses", f"{pct_stale}%")
k5.metric(
    "Passenger Risk",
    "HIGH" if pct_stopped + pct_stale > 60
    else "MEDIUM" if pct_stopped + pct_stale > 30
    else "LOW"
)

# =====================================
# Passenger explanation
# =====================================
st.markdown("## 🧍 Passenger Experience")

st.write(
    f"• **{pct_stopped}%** of buses are currently stopped\n"
    f"• **{pct_stale}%** of buses have not updated recently\n"
    f"• **{congested_routes} routes** show congestion\n\n"
    "High stale or stopped percentages indicate longer waiting times."
)

# =====================================
# Route & Trip filters
# =====================================
st.markdown("## 🔍 Filter View")

f1, f2 = st.columns(2)
routes = ["All"] + sorted(vehicle_df["route_id"].dropna().unique())
selected_route = f1.selectbox("Filter by Route", routes)
trip_search = f2.text_input("Filter by Trip ID")

if selected_route != "All":
    vehicle_df = vehicle_df[vehicle_df["route_id"] == selected_route]
    trip_df = trip_df[trip_df["route_id"] == selected_route]

if trip_search:
    trip_df = trip_df[trip_df["trip_id"].str.contains(trip_search, case=False, na=False)]

# =====================================
# Map
# =====================================
st.markdown("## 🗺 Live Vehicle Map")

st.map(vehicle_df.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]])

# =====================================
# Stopped vehicles table
# =====================================
st.markdown("## ⛔ Long-Stopped Vehicles")

long_stops = stopped_buses[
    stopped_buses["seconds_since_update"] > stop_th
].sort_values("seconds_since_update", ascending=False)

display = long_stops[[
    "route_id", "trip_id", "vehicle_id", "seconds_since_update"
]].copy()

display.columns = ["Route", "Trip", "Vehicle", "Stopped Duration (s)"]

st.dataframe(display.head(10), use_container_width=True)

# =====================================
# Route freshness
# =====================================
st.markdown("## 🔄 Route Freshness")

route_freshness = (
    vehicle_df.groupby("route_id")["seconds_since_update"]
    .min()
    .reset_index()
)

route_freshness["Status"] = route_freshness["seconds_since_update"].apply(
    lambda x: "🟢 Fresh" if x < 30 else "🟡 Delayed" if x < stale_th else "🔴 Stale"
)

st.dataframe(route_freshness, use_container_width=True)

# =====================================
# Trip delay risk
# =====================================
st.markdown("## ⏱ Highest Delay Risk Trips")

delay = trip_df.sort_values("avg_speed").head(10)
delay.columns = ["Trip", "Route", "Avg Speed", "Last Update"]

st.dataframe(delay, use_container_width=True)
