import glob
import numpy as np
import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="Weather Analytics", page_icon="🌦️", layout="wide")

MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
          "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
METRICS = {
    "Average temperature (°C)": "avg_temp",
    "Maximum temperature (°C)": "max_temp",
    "Total rainfall (mm)": "total_rain",
    "Average humidity (%)": "avg_humidity",
    "Average wind speed (km/h)": "avg_wind",
}


@st.cache_data
def load_batch():
    monthly = pd.read_parquet("output/batch/monthly")
    yearly = pd.read_parquet("output/batch/yearly")
    extremes = pd.read_parquet("output/batch/extremes")
    return monthly, yearly, extremes


def load_stream():
    frames = []
    for f in glob.glob("output/stream/data/*.parquet"):
        try:
            frames.append(pd.read_parquet(f))
        except Exception:
            pass  # file may still be mid-write
    if not frames:
        return pd.DataFrame()
    return pd.concat(frames).sort_values("window_start")


try:
    monthly, yearly, extremes = load_batch()
except Exception as e:
    st.error("Batch output not found. Run `python src\\spark_batch.py` first.")
    st.caption(str(e))
    st.stop()

# ---------- Sidebar filters ----------
st.sidebar.header("Filters")
all_cities = sorted(monthly.city.unique())
cities = st.sidebar.multiselect("Cities", all_cities, default=all_cities)
y_min, y_max = int(monthly.year.min()), int(monthly.year.max())
years = st.sidebar.slider("Year range", y_min, y_max, (y_min, y_max))

if not cities:
    st.warning("Select at least one city in the sidebar.")
    st.stop()

m = monthly[monthly.city.isin(cities) & monthly.year.between(*years)].copy()
y = yearly[yearly.city.isin(cities) & yearly.year.between(*years)].copy()
m["date"] = pd.to_datetime(dict(year=m.year, month=m.month, day=1))

st.title("🌦️ Weather Analytics & Historical Trend Monitoring")
st.caption("Big Data pipeline: HDFS · Spark · Kafka · Spark Structured Streaming · Streamlit")

tab1, tab2, tab3, tab4 = st.tabs(
    ["Overview", "Trends", "City comparison", "Live stream"])

# ---------- Tab 1: Overview ----------
with tab1:
    city_stats = m.groupby("city").agg(
        avg_temp=("avg_temp", "mean"),
        avg_humidity=("avg_humidity", "mean"),
        avg_wind=("avg_wind", "mean"))
    annual_rain = y.groupby("city").total_rain.mean()

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Hottest (avg)", city_stats.avg_temp.idxmax(),
              f"{city_stats.avg_temp.max():.1f} °C")
    c2.metric("Wettest (per year)", annual_rain.idxmax(),
              f"{annual_rain.max():.0f} mm")
    c3.metric("Most humid", city_stats.avg_humidity.idxmax(),
              f"{city_stats.avg_humidity.max():.1f} %")
    c4.metric("Windiest", city_stats.avg_wind.idxmax(),
              f"{city_stats.avg_wind.max():.1f} km/h")

    st.plotly_chart(
        px.line(y.sort_values("year"), x="year", y="avg_temp", color="city",
                markers=True, title="Yearly average temperature (°C)"),
        width="stretch")

    st.subheader("Warming trend per city")
    rows = []
    for city, g in y.groupby("city"):
        if len(g) >= 3:
            slope = np.polyfit(g.year, g.avg_temp, 1)[0]
            rows.append({"City": city, "Trend (°C per year)": round(slope, 3),
                         "Change over period (°C)": round(slope * (g.year.max() - g.year.min()), 2)})
    if rows:
        st.dataframe(pd.DataFrame(rows).sort_values("Trend (°C per year)", ascending=False),
                     width="stretch", hide_index=True)

# ---------- Tab 2: Trends ----------
with tab2:
    label = st.selectbox("Metric", list(METRICS))
    col = METRICS[label]
    st.plotly_chart(
        px.line(m.sort_values("date"), x="date", y=col, color="city",
                title=f"Monthly {label}"),
        width="stretch")

    seasonal = m.groupby(["city", "month"])[col].mean().reset_index()
    seasonal["month_name"] = seasonal.month.map(lambda i: MONTHS[i - 1])
    st.plotly_chart(
        px.line(seasonal, x="month_name", y=col, color="city", markers=True,
                category_orders={"month_name": MONTHS},
                title=f"Seasonal pattern: {label}"),
        width="stretch")

# ---------- Tab 3: City comparison ----------
with tab3:
    left, right = st.columns(2)
    left.plotly_chart(
        px.bar(y, x="year", y="total_rain", color="city", barmode="group",
               title="Annual rainfall (mm)"),
        width="stretch")

    pivot = m.groupby(["city", "month"]).avg_temp.mean().unstack()
    pivot.columns = [MONTHS[i - 1] for i in pivot.columns]
    right.plotly_chart(
        px.imshow(pivot, aspect="auto", color_continuous_scale="RdYlBu_r",
                  title="Average temperature by month (°C)"),
        width="stretch")

    st.subheader("Record highs and lows (full dataset)")
    st.dataframe(extremes[extremes.city.isin(cities)],
                 width="stretch", hide_index=True)

# ---------- Tab 4: Live stream ----------
with tab4:
    st.caption("Simulated sensor readings from Kafka, processed by Spark Structured "
               "Streaming in 30-second windows. Refreshes every 10 seconds.")

    @st.fragment(run_every=10)
    def live_view():
        live = load_stream()
        if live.empty:
            st.info("No live data yet. Start the producer and the streaming job.")
            return
        live = live[live.city.isin(cities)]
        latest = live.groupby("city").tail(1).sort_values("city")
        cols = st.columns(max(len(latest), 1))
        for c, (_, r) in zip(cols, latest.iterrows()):
            c.metric(r["city"], f"{r['avg_temp']} °C", f"{r['avg_humidity']}% humidity")
        st.plotly_chart(
            px.line(live, x="window_start", y="avg_temp", color="city",
                    title="Live average temperature (°C)"),
            width="stretch")
        st.plotly_chart(
            px.line(live, x="window_start", y="max_wind", color="city",
                    title="Live max wind speed (km/h)"),
            width="stretch")
        st.caption(f"Last window: {live.window_start.max()}  |  {len(live)} windows loaded")

    live_view()