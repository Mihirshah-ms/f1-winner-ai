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
