import os
import psycopg2
import pandas as pd
import streamlit as st
from datetime import datetime

# -----------------------------
# Page Config
# -----------------------------
st.set_page_config(
    page_title="F1 Data Health & Predictions",
    layout="wide",
)

st.title("🏎️ Formula 1 Data Health Dashboard")

# -----------------------------
# Database Connection
# -----------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("DATABASE_URL not set")
    st.stop()

@st.cache_resource
def get_conn():
    return psycopg2.connect(DATABASE_URL)

conn = get_conn()

# -----------------------------
# Helpers
# -----------------------------
def load_df(query):
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(str(e))
        return pd.DataFrame()

def bool_icon(val):
    return "✅" if val else "❌"

# -----------------------------
# Season Status
# -----------------------------
st.header("📅 Season Completion Status")

season_status = load_df("""
SELECT
    season,
    total_races,
    completed_races,
    pending_races,
    season_complete
FROM f1_season_status
ORDER BY season;
""")

if season_status.empty:
    st.warning("No season data available yet.")
else:
    season_status_display = season_status.copy()
    season_status_display["season_complete"] = season_status_display["season_complete"].apply(bool_icon)
    st.dataframe(season_status_display, use_container_width=True)

# -----------------------------
# Data Health per Race
# -----------------------------
st.header("🩺 Race Data Health")

health_df = load_df("""
SELECT
    season,
    round,
    race_name,
    race_date,
    has_fp1,
    has_fp2,
    has_fp3,
    has_qualy,
    has_race,
    has_sprint_qualy,
    has_sprint_race
FROM f1_data_health
ORDER BY season DESC, round DESC;
""")

if health_df.empty:
    st.warning("No race health data available.")
else:
    display_df = health_df.copy()

    for col in [
        "has_fp1", "has_fp2", "has_fp3",
        "has_qualy", "has_race",
        "has_sprint_qualy", "has_sprint_race"
    ]:
        display_df[col] = display_df[col].apply(bool_icon)

    display_df["race_date"] = pd.to_datetime(
        display_df["race_date"], errors="coerce"
    ).dt.strftime("%d %B %Y")

    st.dataframe(display_df, use_container_width=True)

# -----------------------------
# Upcoming / Latest Race
# -----------------------------
st.header("🏁 Latest / Upcoming Race")

race_info = load_df("""
SELECT
    season,
    round,
    race_name,
    race_date,
    race_time,
    circuit_name,
    circuit_country
FROM f1_races
ORDER BY season DESC, round DESC
LIMIT 1;
""")

if race_info.empty:
    st.warning("No race information available.")
else:
    race = race_info.iloc[0]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Season", int(race["season"]))
        st.metric("Round", int(race["round"]))

    with col2:
        date_str = (
            pd.to_datetime(race["race_date"], errors="coerce")
            .strftime("%d %B %Y")
            if race["race_date"] else "TBD"
        )
        st.metric("Race Date", date_str)
        st.metric("Race Time", race["race_time"] or "TBD")

    with col3:
        st.metric("Circuit", race["circuit_name"])
        st.metric("Country", race["circuit_country"])

# -----------------------------
# Model Status (Safe)
# -----------------------------
st.header("🤖 ML Model Status")

model_exists = os.path.exists("model.pkl")

if model_exists:
    st.success("Model trained and available")
else:
    st.warning("Model not trained yet (auto pipeline will handle this)")

st.info(
    """
**Important**  
The model only trains when:
- Race results exist
- Qualifying exists
- At least 1 completed race

Until then, the dashboard stays stable and usable.
"""
)

# -----------------------------
# Footer
# -----------------------------
st.markdown("---")
st.caption("F1 Analytics Platform • Fully Automated • Zero Manual Intervention")