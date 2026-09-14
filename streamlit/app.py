"""Operational dashboard for finalized Spark gold-layer metrics."""

import os
from pathlib import Path

import pandas as pd
import streamlit as st


DATA_PATH = Path(os.getenv("GOLD_PATH", "data/gold/event_metrics"))


st.set_page_config(page_title="StreamCart Signal", page_icon="◈", layout="wide")

st.markdown(
    """
    <style>
      .stApp { background: #f4f7fb; color: #172033; }
      .block-container { max-width: 1280px; padding-top: 2.4rem; }
      .signal-kicker { color: #59677e; font: 600 0.72rem/1.2 monospace; letter-spacing: .12em; }
      .signal-title { font: 700 clamp(2rem, 4vw, 3.75rem)/.95 Georgia, serif; letter-spacing: -.045em; margin: .25rem 0 .9rem; }
      .signal-rule { height: 4px; background: linear-gradient(90deg, #f04b3a 0 21%, #152238 21% 100%); margin: 1.5rem 0 2rem; }
      [data-testid="stMetric"] { background: #ffffff; border-top: 3px solid #152238; padding: 1rem; }
      [data-testid="stMetricLabel"] { color: #59677e; font-family: monospace; letter-spacing: .06em; text-transform: uppercase; }
      .empty-state { border-left: 5px solid #f04b3a; background: #ffffff; padding: 1.25rem 1.4rem; }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=15)
def load_metrics(path: str) -> pd.DataFrame:
    # Spark writes through a temporary directory during overwrite. Only read
    # finalized files at the gold root, never files under `_temporary`.
    parquet_files = sorted(Path(path).glob("part-*.parquet"))
    frames = []
    for parquet_file in parquet_files:
        try:
            if parquet_file.exists():
                frames.append(pd.read_parquet(parquet_file))
        except (FileNotFoundError, OSError):
            # Spark may replace this file between the directory scan and read.
            continue

    if not frames:
        return pd.DataFrame()

    metrics = pd.concat(frames, ignore_index=True)
    metrics["window_start"] = pd.to_datetime(metrics["window_start"], utc=True)
    metrics["window_end"] = pd.to_datetime(metrics["window_end"], utc=True)
    return metrics


def format_currency(value: float) -> str:
    return f"${value:,.0f}"


st.markdown('<p class="signal-kicker">STREAMCART / GOLD-LAYER SIGNAL</p>', unsafe_allow_html=True)
st.markdown('<h1 class="signal-title">What shoppers are doing, window by window.</h1>', unsafe_allow_html=True)
st.markdown('<div class="signal-rule"></div>', unsafe_allow_html=True)

metrics = load_metrics(str(DATA_PATH))
if metrics.empty:
    st.markdown(
        """
        <div class="empty-state"><strong>No finalized windows yet.</strong><br>
        Start the Spark pipeline and replay events. The dashboard refreshes automatically when
        gold-layer Parquet files arrive.</div>
        """,
        unsafe_allow_html=True,
    )
    st.code("docker compose --profile pipeline up spark", language="bash")
    st.stop()

event_options = sorted(metrics["event_type"].dropna().unique())
selected_events = st.multiselect("Event types", event_options, default=event_options)
filtered = metrics[metrics["event_type"].isin(selected_events)].copy()

event_total = int(filtered["event_count"].sum())
purchases = int(filtered.loc[filtered["event_type"] == "purchase", "event_count"].sum())
views = int(filtered.loc[filtered["event_type"] == "view", "event_count"].sum())
revenue = float(filtered["purchase_revenue"].sum())
conversion = (purchases / views * 100) if views else 0.0

metric_columns = st.columns(4)
metric_columns[0].metric("Observed events", f"{event_total:,}")
metric_columns[1].metric("Purchases", f"{purchases:,}")
metric_columns[2].metric("Purchase revenue", format_currency(revenue))
metric_columns[3].metric("View → purchase", f"{conversion:.2f}%")

left, right = st.columns((1.4, 1), gap="large")
with left:
    st.subheader("Event pulse")
    pulse = (
        filtered.groupby(["window_start", "event_type"], as_index=False)["event_count"]
        .sum()
        .pivot(index="window_start", columns="event_type", values="event_count")
        .fillna(0)
        .sort_index()
    )
    st.line_chart(pulse, use_container_width=True)

with right:
    st.subheader("Brands in motion")
    brands = (
        filtered.groupby("brand", as_index=False)["event_count"]
        .sum()
        .sort_values("event_count", ascending=False)
        .head(10)
        .set_index("brand")
    )
    st.bar_chart(brands, use_container_width=True)

st.subheader("Finalized event windows")
st.dataframe(
    filtered.sort_values("window_start", ascending=False),
    column_config={
        "window_start": st.column_config.DatetimeColumn("Window start", format="DD MMM, HH:mm"),
        "window_end": st.column_config.DatetimeColumn("Window end", format="HH:mm"),
        "event_count": st.column_config.NumberColumn("Events", format="%,d"),
        "active_users": st.column_config.NumberColumn("Active users", format="%,d"),
        "purchase_revenue": st.column_config.NumberColumn("Revenue", format="$%,.2f"),
    },
    hide_index=True,
    use_container_width=True,
)
