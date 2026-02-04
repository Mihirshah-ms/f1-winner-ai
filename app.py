import streamlit as st
import psycopg2
import os
import pandas as pd

st.set_page_config(
    page_title="F1 Analytics Platform",
    page_icon="🏎️",
    layout="wide"
)

st.title("🏎️ Formula 1 Analytics Platform")

# -------------------------------
# Database connection
# -------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")

@st.cache_resource
def get_conn():
    return psycopg2.connect(DATABASE_URL)

conn = get_conn()

# -------------------------------
# Latest / Upcoming Race
# -------------------------------
st.subheader("🏁 Latest / Upcoming Race")

try:
    df_race = pd.read_sql(
        """
        SELECT
            r.season,
            r.round,
            r.race_name,
            r.race_date,
            r.race_time,
            r.circuit_name,
            r.circuit_country
        FROM f1_races r
        LEFT JOIN f1_race_results rr
            ON r.season = rr.season
            AND r.round = rr.round
        WHERE r.season = 2026
        AND rr.season IS NULL
        ORDER BY r.round ASC
        LIMIT 1;
        """,
        conn
    )

    if df_race.empty:
        st.info("No race data available yet.")
    else:
        r = df_race.iloc[0]

        col1, col2 = st.columns(2)

        with col1:
            st.metric("Season", r["season"])
            st.metric("Round", r["round"])
            st.metric("Race", r["race_name"])

        with col2:
            st.metric("Date", r["race_date"] or "TBD")
            st.metric("Time", r["race_time"] or "TBD")
            st.metric("Circuit", r["circuit_name"])

        st.caption(f"Country: {r['circuit_country']}")

except Exception as e:
    st.error("Failed to load race information.")
    st.code(str(e))

# -------------------------------
# ML Model Status (lightweight)
# -------------------------------
st.subheader("🤖 ML Model Status")

st.info(
    """
The prediction model will train automatically when:
• Qualifying data exists  
• Race results exist  
• At least one race is completed  

Until then, the platform stays live and stable.
"""
)

# -------------------------------
# Footer
# -------------------------------
st.markdown("---")
st.caption("F1 Analytics Platform • Stable Mode • Dashboard Removed")
