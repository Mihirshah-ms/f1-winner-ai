import streamlit as st
import os
import psycopg2
import pandas as pd
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score

# -------------------------------------------------
# PAGE CONFIG
# -------------------------------------------------
st.set_page_config(page_title="F1 Winner AI", layout="wide")
st.title("🏎️ F1 Winner AI")
st.caption("AI-powered Formula 1 race winner prediction")

# -------------------------------------------------
# DB CONNECTION
# -------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)

# -------------------------------------------------
# LOAD TRAINING DATA
# -------------------------------------------------
df_train = pd.read_sql("""
SELECT
    season,
    round,
    driver_id,
    qualy_score,
    constructor_score,
    avg_driver_form,
    winner
FROM f1_training_data
""", conn)

st.write(f"📊 Training rows available: {len(df_train)}")

# -------------------------------------------------
# TRAIN MODEL
# -------------------------------------------------
X = df_train[["qualy_score", "constructor_score", "avg_driver_form"]]
y = df_train["winner"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

model = GradientBoostingClassifier(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=3,
    random_state=42
)

model.fit(X_train, y_train)

preds = model.predict(X_test)
accuracy = accuracy_score(y_test, preds)

st.success(f"✅ Model trained successfully — Accuracy: {accuracy:.2f}")

# -------------------------------------------------
# FEATURE IMPORTANCE
# -------------------------------------------------
st.subheader("🔍 Model Feature Importance")

features = ["Qualifying Performance", "Constructor Strength", "Recent Driver Form"]
importances = model.feature_importances_

for name, imp in sorted(zip(features, importances), key=lambda x: x[1], reverse=True):
    st.write(f"• **{name}** → `{imp:.2f}`")

# -------------------------------------------------
# FIND NEXT RACE
# -------------------------------------------------
# -------------------------------------------------
# BASELINE SEASON (LOCKED TO 2025)
# -------------------------------------------------
BASELINE_SEASON = 2025

latest_2025 = pd.read_sql(f"""
SELECT MAX(round) AS round
FROM f1_training_data
WHERE season = {BASELINE_SEASON}
""", conn).iloc[0]

if latest_2025["round"] is not None:
    baseline_round = int(latest_2025["round"])
else:
    # Fallback to race results if training data not ready yet
    fallback = pd.read_sql(f"""
    SELECT MAX(round) AS round
    FROM f1_race_results
    WHERE season = {BASELINE_SEASON}
    """, conn).iloc[0]

    baseline_round = int(fallback["round"])


# Prediction season is NEXT year
PREDICTION_SEASON = BASELINE_SEASON + 1
PREDICTION_ROUND = 1

st.divider()
st.header("🏆 Predicted Winner — Next Race")
# -------------------------------------------------
# NEXT RACE DETAILS (WHEN & WHERE)
# -------------------------------------------------
# -------------------------------------------------
# NEXT RACE DETAILS (WHEN & WHERE)
# -------------------------------------------------

# Always define race_info first
race_info = pd.DataFrame()

try:
    race_info = pd.read_sql(f"""
    SELECT
        race_name,
        race_date,
        race_time,
        circuit_name,
        circuit_country
    FROM f1_races
    WHERE season = {season}
      AND round = {next_round}
    """, conn)
except Exception:
    race_info = pd.DataFrame()

# Fallback to latest race if next race not available
if race_info.empty:
    race_info = pd.read_sql(f"""
    SELECT
        race_name,
        race_date,
        race_time,
        circuit_name,
        circuit_country
    FROM f1_races
    WHERE season = {season}
      AND round = {latest["round"]}
    """, conn)

race = race_info.iloc[0]

from datetime import datetime

# Format race date nicely
race_date = race["race_date"]
if isinstance(race_date, str):
    race_date = datetime.strptime(race_date, "%Y-%m-%d")

formatted_date = race_date.strftime("%d %B %Y")

st.markdown(
    f"""
📍 **{race['race_name']}**  
🌍 {race['circuit_name']}, {race['circuit_country']}  
🗓️ {formatted_date}  
⏰ {race['race_time']} UTC
"""
)

# -------------------------------------------------
# LOAD FEATURES FOR NEXT RACE
# -------------------------------------------------
df_next = pd.read_sql(f"""
WITH latest_team AS (
    SELECT DISTINCT ON (driver_id)
        driver_id,
        team_id
    FROM f1_race_results
    WHERE season = {BASELINE_SEASON}
    ORDER BY driver_id, round DESC
)
SELECT
    q.driver_id,
    q.qualy_score,
    c.constructor_score,
    d.avg_finish_5 AS avg_driver_form
FROM f1_qualifying_features q
JOIN latest_team lt
  ON q.driver_id = lt.driver_id
JOIN f1_constructor_strength c
  ON c.team_id = lt.team_id
 AND c.season = {BASELINE_SEASON}
 AND c.round = {baseline_round}
JOIN f1_driver_recent_form d
  ON d.driver_id = q.driver_id
 AND d.season = {BASELINE_SEASON}
 AND d.round = {baseline_round}
WHERE q.season = {PREDICTION_SEASON}
  AND q.round = {PREDICTION_ROUND}
""", conn)

conn.close()

if df_next.empty:
    st.warning("⚠️ Next-race data not available yet. Showing prediction using latest race data instead.")

    fallback_conn = psycopg2.connect(os.getenv("DATABASE_URL"))

    df_next = pd.read_sql(f"""
    WITH latest_team AS (
        SELECT DISTINCT ON (driver_id)
            driver_id,
            team_id
        FROM f1_race_results
        ORDER BY driver_id, season DESC, round DESC
    )
    SELECT
        q.driver_id,
        q.qualy_score,
        c.constructor_score,
        d.avg_finish_5 AS avg_driver_form
    FROM f1_qualifying_features q
    JOIN latest_team lt
      ON q.driver_id = lt.driver_id
    JOIN f1_constructor_strength c
      ON c.team_id = lt.team_id
     AND c.season = q.season
     AND c.round = q.round
    JOIN f1_driver_recent_form d
      ON q.season = d.season
     AND q.round = d.round
     AND q.driver_id = d.driver_id
    WHERE q.season = {season}
      AND q.round = {latest["round"]}
    """, fallback_conn)

    fallback_conn.close()

# Continue prediction as normal below
    # -------------------------------------------------
    # PREDICT WIN PROBABILITIES
    # -------------------------------------------------
    df_next["win_probability"] = model.predict_proba(
        df_next[["qualy_score", "constructor_score", "avg_driver_form"]]
    )[:, 1]

    df_next = df_next.sort_values("win_probability", ascending=False)

    winner = df_next.iloc[0]

    # -------------------------------------------------
    # DISPLAY PREDICTION
    # -------------------------------------------------
    st.success(
        f"🥇 **{winner['driver_id'].upper()}** "
        f"— {winner['win_probability']*100:.1f}% chance of winning"
    )

    # -------------------------------------------------
    # CONFIDENCE MESSAGE
    # -------------------------------------------------
    confidence = winner["win_probability"]

    if confidence >= 0.6:
        st.info("🔒 **High confidence prediction** — strong advantage expected")
    elif confidence >= 0.4:
        st.info("⚖️ **Medium confidence** — competitive race expected")
    else:
        st.info("⚠️ **Low confidence** — unpredictable race")

    # -------------------------------------------------
    # TOP CONTENDERS TABLE
    # -------------------------------------------------
    st.subheader("📈 Top Contenders")
    st.dataframe(
        df_next[["driver_id", "win_probability"]]
        .head(10)
        .assign(win_probability=lambda x: (x.win_probability * 100).round(2))
        .rename(columns={"win_probability": "Win Probability (%)"})
    )

st.divider()
st.caption("Built with ❤️ using real F1 data, ML, and full automation")

# -------------------------------------------------
# CLOSE DB CONNECTION (END OF APP)
# -------------------------------------------------
if conn:
    conn.close()
