import streamlit as st
import os
import psycopg2
import pandas as pd
import pickle
from datetime import datetime

# ---------------- PAGE SETUP ----------------
st.set_page_config(
    page_title="F1 Winner AI",
    page_icon="🏎️",
    layout="wide"
)

st.title("🏁 F1 Winner AI")
st.caption("AI-powered race winner & podium prediction • Fully automated")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set")
    st.stop()

# ---------------- LOAD MODEL ----------------
if not os.path.exists("model.pkl"):
    st.error("❌ Model not trained yet. Wait for auto-pipeline cron run.")
    st.stop()

with open("model.pkl", "rb") as f:
    model = pickle.load(f)

# ---------------- DB CONNECTION ----------------
conn = psycopg2.connect(DATABASE_URL)

# ---------------- MODEL METADATA ----------------
log = pd.read_sql("""
SELECT run_time, accuracy
FROM model_logs
ORDER BY run_time DESC
LIMIT 1;
""", conn)

if not log.empty:
    run_time = log.iloc[0]["run_time"]
    accuracy = log.iloc[0]["accuracy"]
else:
    run_time = None
    accuracy = None

# ---------------- NEXT RACE INFO ----------------
race = pd.read_sql("""
SELECT season, round, race_name, race_date,
       circuit_name, circuit_country
FROM f1_races
ORDER BY season DESC, round DESC
LIMIT 1;
""", conn).iloc[0]

if race["race_date"]:
    race_date = pd.to_datetime(race["race_date"]).strftime("%d %B %Y")
else:
    race_date = "TBA"

# ---------------- FEATURES FOR PREDICTION ----------------
features = pd.read_sql(f"""
SELECT
    q.driver_id,
    q.qualy_score,
    d.avg_finish_5,
    c.avg_team_finish_24
FROM f1_qualifying_features q
LEFT JOIN f1_driver_recent_form d
  ON q.driver_id = d.driver_id
 AND q.season = d.season
 AND q.round = d.round
LEFT JOIN f1_constructor_strength c
  ON q.season = c.season
 AND q.round = c.round
WHERE q.season = {race['season']}
  AND q.round = {race['round']};
""", conn)

conn.close()

if features.empty:
    st.warning("⚠️ Prediction data not available yet.")
    st.stop()

X = features[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
features["win_probability"] = model.predict_proba(X)[:, 1]

# ---------------- SORT ----------------
features = features.sort_values("win_probability", ascending=False)

# ---------------- HEADER ----------------
st.subheader("🏎️ Upcoming Race")
st.markdown(
    f"""
**Race:** {race['race_name']}  
**Date:** {race_date}  
**Location:** {race['circuit_name']}, {race['circuit_country']}
"""
)

# ---------------- METRICS ----------------
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("🏆 Predicted Winner", features.iloc[0]["driver_id"].upper())

with col2:
    st.metric(
        "📈 Win Probability",
        f"{features.iloc[0]['win_probability']*100:.1f}%"
    )

with col3:
    if accuracy:
        st.metric(
            "🧠 Model Accuracy",
            f"{accuracy*100:.1f}%",
            help=f"Last trained: {run_time}"
        )
    else:
        st.metric("🧠 Model Accuracy", "N/A")

# ---------------- PODIUM ----------------
st.subheader("🥉 Podium Probabilities")

podium = features.head(3).copy()
podium["Win Probability (%)"] = (podium["win_probability"] * 100).round(2)

st.table(
    podium[["driver_id", "Win Probability (%)"]]
    .rename(columns={"driver_id": "Driver"})
)

# ---------------- TOP 5 ----------------
st.subheader("📊 Top 5 Win Probabilities")

top5 = features.head(5).copy()
top5["Win Probability (%)"] = (top5["win_probability"] * 100).round(2)

st.dataframe(
    top5[["driver_id", "Win Probability (%)"]]
    .rename(columns={"driver_id": "Driver"}),
    use_container_width=True
)

# ---------------- FOOTER ----------------
st.caption(
    f"Last model update: {run_time if run_time else 'Pending'} • "
    "System retrains automatically via cron"
)