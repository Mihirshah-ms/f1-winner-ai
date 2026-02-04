import streamlit as st
import os
import psycopg2
import pandas as pd
import pickle

# ---------------- PAGE CONFIG ----------------
st.set_page_config(
    page_title="F1 Winner AI",
    page_icon="🏎️",
    layout="wide"
)

st.title("🏁 F1 Winner AI")
st.caption("Fully automated race & championship prediction system")

# ---------------- ENV ----------------
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set")
    st.stop()

# ---------------- DB CONNECTION ----------------
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ---------------- LOAD MODEL FROM DB ----------------
cur.execute("""
SELECT model_blob, trained_at, accuracy
FROM ml_models
WHERE model_name = 'f1_winner_model'
LIMIT 1;
""")

row = cur.fetchone()

if not row:
    st.warning("⚠️ Model not trained yet. Please wait for cron job to run.")
    st.stop()

model = pickle.loads(row[0])
model_trained_at = row[1]
model_accuracy = row[2]

# ---------------- LATEST RACE ----------------
race_df = pd.read_sql("""
SELECT season, round, race_name, race_date,
       circuit_name, circuit_country
FROM f1_races
ORDER BY season DESC, round DESC
LIMIT 1;
""", conn)

if race_df.empty:
    st.warning("No race data available.")
    st.stop()

race = race_df.iloc[0]

race_date = (
    pd.to_datetime(race["race_date"]).strftime("%d %B %Y")
    if race["race_date"] else "TBA"
)

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

if features.empty:
    st.warning("Prediction data not available yet.")
    st.stop()

X = features[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
features["win_probability"] = model.predict_proba(X)[:, 1]
features = features.sort_values("win_probability", ascending=False)

# ---------------- RACE INFO UI ----------------
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
    st.metric(
        "🧠 Model Accuracy",
        f"{model_accuracy*100:.1f}%",
        help=f"Last trained: {model_trained_at}"
    )

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

# ---------------- CHAMPIONSHIP ----------------
st.subheader("🏆 Championship Prediction")

champ = pd.read_sql("""
SELECT
  driver_id,
  current_points,
  expected_future_points,
  projected_total_points,
  championship_probability
FROM f1_championship_projection
ORDER BY championship_probability DESC;
""", conn)

if not champ.empty:
    champ["Championship Probability (%)"] = (champ["championship_probability"] * 100).round(2)

    st.dataframe(
        champ.rename(columns={
            "driver_id": "Driver",
            "current_points": "Current Pts",
            "expected_future_points": "Expected Pts",
            "projected_total_points": "Projected Total"
        }),
        use_container_width=True
    )
else:
    st.info("Championship prediction not available yet.")

# ---------------- FOOTER ----------------
st.caption(
    f"Model last trained: {model_trained_at} • "
    "System retrains automatically via daily cron"
)

conn.close()