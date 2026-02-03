import streamlit as st
import os
import psycopg2
import pandas as pd

from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.impute import SimpleImputer

# ---------------- UI SETUP ----------------
st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🏆 F1 Winner AI — Next Race Prediction (2026)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

st.write(
    "This dashboard predicts the **winner of the next Formula 1 race** "
    "using machine learning trained on 2025 season data."
)

# ---------------- DB CONNECTION ----------------
conn = psycopg2.connect(DATABASE_URL)

# ---------------- FIND BASELINE SEASON & ROUND ----------------
latest = pd.read_sql("""
SELECT MAX(season) AS season, MAX(round) AS round
FROM f1_training_data
WHERE season = 2025;
""", conn).iloc[0]

baseline_season = int(latest["season"])
baseline_round = int(latest["round"])

st.info(f"📊 Model trained using **Season {baseline_season}, Round {baseline_round}** as baseline")

# ---------------- LOAD TRAINING DATA ----------------
train_df = pd.read_sql("""
SELECT
    qualy_score,
    avg_finish_5,
    avg_team_finish_24,
    winner
FROM f1_training_data
WHERE season = 2025
  AND winner IS NOT NULL;
""", conn)

X_train = train_df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
y_train = train_df["winner"]

# ---------------- TRAIN MODEL (ON LOAD) ----------------
pipeline = Pipeline(steps=[
    ("imputer", SimpleImputer(strategy="median", add_indicator=True)),
    ("model", LogisticRegression(max_iter=1000))
])

pipeline.fit(X_train, y_train)

# ---------------- DETERMINE NEXT RACE ----------------
next_race = pd.read_sql("""
SELECT season, round, race_name, race_date, circuit_name, circuit_country
FROM f1_races
WHERE season = 2026
ORDER BY round
LIMIT 1;
""", conn)

use_fallback = False

if next_race.empty:
    use_fallback = True
    next_race = pd.read_sql("""
    SELECT season, round, race_name, race_date, circuit_name, circuit_country
    FROM f1_races
    WHERE season = 2025
    ORDER BY round DESC
    LIMIT 1;
    """, conn)

race = next_race.iloc[0]

# ---------------- DISPLAY RACE INFO ----------------
st.subheader("🏁 Upcoming Race")

race_date = pd.to_datetime(race["race_date"]).strftime("%d %B %Y")

st.write(f"**Race:** {race['race_name']}")
st.write(f"**Date:** {race_date}")
st.write(f"**Location:** {race['circuit_name']}, {race['circuit_country']}")

if use_fallback:
    st.warning("⚠️ 2026 race data not fully available yet. Using latest 2025 race context.")

# ---------------- LOAD FEATURES FOR PREDICTION ----------------
features_df = pd.read_sql(f"""
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
WHERE q.season = {baseline_season}
  AND q.round = {baseline_round};
""", conn)

conn.close()

if features_df.empty:
    st.error("❌ Prediction data not available yet.")
    st.stop()

X_pred = features_df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]

# ---------------- PREDICT ----------------
probs = pipeline.predict_proba(X_pred)[:, 1]
features_df["win_probability"] = probs

winner_row = features_df.sort_values("win_probability", ascending=False).iloc[0]

# ---------------- DISPLAY PREDICTION ----------------
st.subheader("🥇 Predicted Winner")

st.success(f"🏆 **{winner_row['driver_id'].upper()}**")

st.write(
    f"📈 **Win Probability:** {winner_row['win_probability']*100:.2f}%"
)

# ---------------- SHOW TOP 5 ----------------
st.subheader("📊 Top 5 Win Probabilities")

top5 = features_df.sort_values("win_probability", ascending=False).head(5)

st.dataframe(
    top5[["driver_id", "win_probability"]]
    .assign(win_probability=lambda x: (x.win_probability * 100).round(2))
    .rename(columns={"win_probability": "Win Probability (%)"})
)