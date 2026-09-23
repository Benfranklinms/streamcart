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
      :root {
        --ink: #17231f;
        --muted: #68756f;
        --paper: #f6f7f3;
        --card: #ffffff;
        --line: #dfe5de;
        --coral: #dc6b4d;
        --moss: #789182;
      }
      .stApp { background: var(--paper); color: var(--ink); }
      .block-container { max-width: 1380px; padding: 3.25rem 3rem 4rem; }
      .signal-kicker { color: var(--coral); font: 700 .68rem/1.2 ui-monospace, SFMono-Regular, monospace; letter-spacing: .16em; margin-bottom: .7rem; }
      .signal-title { color: var(--ink); font: 650 clamp(2.25rem, 4vw, 4.35rem)/.94 Georgia, 'Times New Roman', serif; letter-spacing: -.055em; margin: 0; max-width: 760px; }
      .signal-deck { color: var(--muted); font: 400 1rem/1.55 ui-sans-serif, system-ui, sans-serif; margin: 1rem 0 0; max-width: 520px; }
      .signal-rule { height: 1px; background: var(--line); margin: 2.5rem 0 1.5rem; position: relative; }
      .signal-rule::before { background: var(--coral); content: ''; display: block; height: 4px; left: 0; position: absolute; top: -2px; width: 15%; }
      .section-label { color: var(--muted); font: 700 .68rem/1.2 ui-monospace, SFMono-Regular, monospace; letter-spacing: .14em; text-transform: uppercase; margin: .75rem 0 .5rem; }
      [data-testid="stMetric"] { background: var(--card); border: 1px solid var(--line); border-radius: 14px; box-shadow: 0 8px 24px rgba(23, 35, 31, .045); padding: 1.15rem 1.2rem 1.05rem; }
      [data-testid="stMetricLabel"] { color: var(--muted); font: 700 .66rem/1.2 ui-monospace, SFMono-Regular, monospace; letter-spacing: .1em; text-transform: uppercase; }
      [data-testid="stMetricValue"] { color: var(--ink); font: 650 1.85rem/1.1 ui-sans-serif, system-ui, sans-serif; letter-spacing: -.045em; }
      [data-testid="stMetricDelta"] { font-size: .72rem; }
      [data-testid="stMarkdownContainer"] h2, [data-testid="stMarkdownContainer"] h3 { color: var(--ink); font-family: Georgia, 'Times New Roman', serif; letter-spacing: -.025em; }
      [data-testid="stMarkdownContainer"] h3 { font-size: 1.35rem; margin-top: .25rem; }
      [data-testid="stDataFrame"] { border: 1px solid var(--line); border-radius: 14px; overflow: hidden; box-shadow: 0 8px 24px rgba(23, 35, 31, .04); }
      div[data-baseweb="select"] > div { background: var(--card); border-color: var(--line); border-radius: 10px; }
      .empty-state { background: var(--card); border: 1px solid var(--line); border-left: 4px solid var(--coral); border-radius: 0 14px 14px 0; box-shadow: 0 8px 24px rgba(23, 35, 31, .04); padding: 1.3rem 1.5rem; }
      .empty-state strong { color: var(--ink); }
      @media (max-width: 720px) {
        .block-container { padding: 2rem 1.1rem 3rem; }
        .signal-title { font-size: 2.65rem; }
      }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=15)
def load_metrics(path: str) -> pd.DataFrame:
    # Spark writes through a temporary directory during overwrite. Only read
    # finalized files at the gold root, never files under `_temporary`.
    parquet_files = sorted(
        parquet_file
        for parquet_file in Path(path).rglob("part-*.parquet")
        if "_temporary" not in parquet_file.parts
    )
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
st.markdown('<p class="signal-deck">A quiet read on live commerce behavior—events, intent, and revenue as they settle into the stream.</p>', unsafe_allow_html=True)
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

st.markdown('<p class="section-label">Explore the stream</p>', unsafe_allow_html=True)
event_options = sorted(metrics["event_type"].dropna().unique())
selected_events = st.multiselect("Event types", event_options, default=event_options)
filtered = metrics[metrics["event_type"].isin(selected_events)].copy()

event_total = int(filtered["event_count"].sum())
purchases = int(filtered.loc[filtered["event_type"] == "purchase", "event_count"].sum())
views = int(filtered.loc[filtered["event_type"] == "view", "event_count"].sum())
revenue = float(filtered["purchase_revenue"].sum())
conversion = (purchases / views * 100) if views else 0.0

st.markdown('<p class="section-label">At a glance</p>', unsafe_allow_html=True)
metric_columns = st.columns(4, gap="medium")
metric_columns[0].metric("Observed events", f"{event_total:,}")
metric_columns[1].metric("Purchases", f"{purchases:,}")
metric_columns[2].metric("Purchase revenue", format_currency(revenue))
metric_columns[3].metric("View → purchase", f"{conversion:.2f}%")

left, right = st.columns((1.4, 1), gap="large")
with left:
    with st.container(border=True):
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
    with st.container(border=True):
        st.subheader("Brands in motion")
        brands = (
            filtered.groupby("brand", as_index=False)["event_count"]
            .sum()
            .sort_values("event_count", ascending=False)
            .head(10)
            .set_index("brand")
        )
        st.bar_chart(brands, use_container_width=True)

with st.container(border=True):
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
