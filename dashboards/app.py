import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path

# ==========================================
# PAGE CONFIG
# ==========================================

st.set_page_config(
    page_title="UrbanPulse Dashboard",
    layout="wide"
)

st.title("🌆 UrbanPulse Real-Time City Intelligence")

# ==========================================
# LOAD DATA
# ==========================================

gold_path = "data/gold/stress_index"
alert_path = "data/gold/alerts"

if not Path(gold_path).exists():
    st.error(
        "Stress Index data not found. Run stress_index_engine.py first."
    )
    st.stop()

if not Path(alert_path).exists():
    st.error(
        "Alert data not found. Run alert_engine.py first."
    )
    st.stop()

stress_df = pd.read_parquet(gold_path)
alerts_df = pd.read_parquet(alert_path)

# ==========================================
# METRICS
# ==========================================

st.subheader("📊 Overall Metrics")

col1, col2, col3 = st.columns(3)

col1.metric(
    "Cities",
    len(stress_df)
)

col2.metric(
    "Average Stress Index",
    round(
        stress_df["stress_index"].mean(),
        2
    )
)

high_alerts = len(
    alerts_df[
        alerts_df["alert_level"] == "HIGH"
    ]
)

col3.metric(
    "High Alerts",
    high_alerts
)

# ==========================================
# STRESS INDEX TABLE
# ==========================================

st.subheader("🏙 City Stress Scores")

st.dataframe(
    stress_df,
    use_container_width=True
)

# ==========================================
# STRESS INDEX BAR CHART
# ==========================================

st.subheader("📈 Stress Index by City")

fig = px.bar(
    stress_df,
    x="city",
    y="stress_index",
    title="Stress Index"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# ==========================================
# TEMPERATURE CHART
# ==========================================

st.subheader("🌡 Average Temperature")

fig = px.bar(
    stress_df,
    x="city",
    y="avg_temperature",
    title="Average Temperature"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# ==========================================
# PM25 CHART
# ==========================================

st.subheader("💨 Air Quality (PM2.5)")

fig = px.bar(
    stress_df,
    x="city",
    y="avg_pm25",
    title="Average PM2.5"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# ==========================================
# TRAFFIC CHART
# ==========================================

st.subheader("🚗 Traffic Density")

fig = px.bar(
    stress_df,
    x="city",
    y="avg_traffic",
    title="Traffic Density"
)

st.plotly_chart(
    fig,
    use_container_width=True
)

# ==========================================
# ALERTS
# ==========================================

st.subheader("🚨 Alerts")

st.dataframe(
    alerts_df,
    use_container_width=True
)

# ==========================================
# HIGH ALERT CITIES
# ==========================================

high_df = alerts_df[
    alerts_df["alert_level"] == "HIGH"
]

if len(high_df) > 0:

    st.subheader(
        "🔴 High Risk Cities"
    )

    st.dataframe(
        high_df,
        use_container_width=True
    )

else:
    st.success(
        "No high-risk cities detected."
    )

# ==========================================
# RAW DATA
# ==========================================

with st.expander(
    "View Raw Stress Data"
):
    st.dataframe(
        stress_df,
        use_container_width=True
    )

with st.expander(
    "View Raw Alert Data"
):
    st.dataframe(
        alerts_df,
        use_container_width=True
    )


