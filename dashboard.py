import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import numpy as np
import time

from streamlit_autorefresh import st_autorefresh

# ------------------------------------------------
# PAGE CONFIG
# ------------------------------------------------

st.set_page_config(
    page_title="UrbanPulse Dashboard",
    page_icon="🌆",
    layout="wide"
)

# ------------------------------------------------
# AUTO REFRESH
# ------------------------------------------------

st_autorefresh(
    interval=5000,
    key="urbanpulse_refresh"
)

# ------------------------------------------------
# LOAD DATA
# ------------------------------------------------

@st.cache_data(ttl=5)
def load_city_metrics():

    try:
        return pd.read_parquet(
            "data/gold/city_metrics"
        )

    except Exception:

        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_stress():

    try:
        return pd.read_parquet(
            "data/gold/stress_index"
        )

    except Exception:

        return pd.DataFrame()


@st.cache_data(ttl=5)
def load_alerts():

    try:
        return pd.read_parquet(
            "data/gold/alerts"
        )

    except Exception:

        return pd.DataFrame()


city_metrics = load_city_metrics()
stress_df = load_stress()
alerts_df = load_alerts()

# ------------------------------------------------
# HEADER
# ------------------------------------------------

st.title("🌆 UrbanPulse Live Dashboard")

st.caption(
    "Kafka + Spark + Streamlit Real-Time Monitoring"
)

# ------------------------------------------------
# NO DATA
# ------------------------------------------------

if city_metrics.empty:

    st.error(
        "No Gold Data Found. Run gold_city_metrics.py first."
    )

    st.stop()

# ------------------------------------------------
# KPI SECTION
# ------------------------------------------------

c1, c2, c3, c4 = st.columns(4)

c1.metric(
    "Cities",
    len(city_metrics)
)

c2.metric(
    "Avg Temp",
    round(
        city_metrics["avg_temperature"].mean(),
        1
    )
)

c3.metric(
    "Avg PM2.5",
    round(
        city_metrics["avg_pm25"].mean(),
        1
    )
)

c4.metric(
    "Avg Traffic",
    round(
        city_metrics["avg_traffic"].mean(),
        1
    )
)

st.divider()

# ------------------------------------------------
# CITY SELECT
# ------------------------------------------------

selected_city = st.selectbox(
    "Select City",
    city_metrics["city"].tolist()
)

city_row = city_metrics[
    city_metrics["city"] == selected_city
]

# ------------------------------------------------
# CHART ROW 1
# ------------------------------------------------

col1, col2 = st.columns(2)

with col1:

    fig = px.bar(
        city_metrics,
        x="city",
        y="avg_temperature",
        color="avg_temperature",
        title="🌡 Temperature Comparison"
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key="temp_bar"
    )

with col2:

    fig = px.pie(
        city_metrics,
        values="avg_pm25",
        names="city",
        title="🏭 PM2.5 Distribution"
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key="pm_pie"
    )

# ------------------------------------------------
# CHART ROW 2
# ------------------------------------------------

col3, col4 = st.columns(2)

with col3:

    fig = px.scatter(
        city_metrics,
        x="avg_temperature",
        y="avg_pm25",
        size="avg_traffic",
        color="city",
        title="🌆 City Health Scatter"
    )

    st.plotly_chart(
        fig,
        use_container_width=True,
        key="scatter"
    )

with col4:

    if not stress_df.empty and "stress_index" in stress_df.columns:

        try:

            stress_value = float(
                stress_df[
                    stress_df["city"]
                    == selected_city
                ]["stress_index"].iloc[0]
            )

        except:

            stress_value = 0

    else:

        stress_value = (
            city_row["avg_temperature"].iloc[0]
            * 0.3
            +
            city_row["avg_pm25"].iloc[0]
            * 0.4
            +
            city_row["avg_traffic"].iloc[0]
            * 0.3
        )

    gauge = go.Figure(
        go.Indicator(
            mode="gauge+number",
            value=stress_value,
            title={
                "text":
                f"{selected_city} Stress Index"
            },
            gauge={
                "axis": {
                    "range": [0, 100]
                },
                "steps": [
                    {
                        "range": [0, 40],
                        "color": "green"
                    },
                    {
                        "range": [40, 70],
                        "color": "orange"
                    },
                    {
                        "range": [70, 100],
                        "color": "red"
                    }
                ]
            }
        )
    )

    st.plotly_chart(
        gauge,
        use_container_width=True,
        key="stress_gauge"
    )

# --------------------------------
# REAL TIME STRESS TREND
# --------------------------------

st.divider()

st.subheader(
    f"📈 {selected_city} Real-Time Stress Trend"
)

try:

    history = pd.read_parquet(
        "data/gold/city_metrics_history"
    )

    city_history = history[
        history["city"] == selected_city
    ]

    city_history = city_history.sort_values(
        "timestamp"
    )

    trend_fig = px.line(
        city_history,
        x="timestamp",
        y="stress_index",
        markers=True,
        title=f"{selected_city} Stress Index Over Time"
    )

    trend_fig.update_layout(
        xaxis_title="Time",
        yaxis_title="Stress Index",
        height=400
    )

    st.plotly_chart(
        trend_fig,
        use_container_width=True,
        key="stress_trend"
    )

except Exception:

    st.info(
        "Waiting for historical streaming data..."
    )

# ------------------------------------------------
# CITY TABLE
# ------------------------------------------------

st.divider()

st.subheader(
    f"📍 {selected_city} Metrics"
)

st.dataframe(
    city_row,
    use_container_width=True
)

# ------------------------------------------------
# ALERTS
# ------------------------------------------------

st.divider()

st.subheader("🚨 Alerts")

if alerts_df.empty:

    st.info(
        "No alerts generated yet."
    )

else:

    st.dataframe(
        alerts_df,
        use_container_width=True
    )

# ------------------------------------------------
# STRESS RANKING
# ------------------------------------------------

st.divider()

st.subheader(
    "📊 City Ranking"
)

ranking = city_metrics.sort_values(
    "avg_traffic",
    ascending=False
)

st.dataframe(
    ranking,
    use_container_width=True
)

# ------------------------------------------------
# FOOTER
# ------------------------------------------------

st.caption(
    "UrbanPulse • Real-Time Smart City Analytics"
)


