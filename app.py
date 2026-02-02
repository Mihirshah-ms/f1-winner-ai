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
latest = pd.read_sql("""
SELECT MAX(season) AS season, MAX(round) AS round
FROM f1_training_data
""", conn).iloc[0]

season = int(latest["season"])
next_round = int(latest["round"]) + 1

st.divider()
st.header("🏆 Predicted Winner — Next Race")

# -------------------------------------------------
# LOAD FEATURES FOR NEXT RACE
# -------------------------------------------------
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
  AND q.round = {next_round}
""", conn)

conn.close()

if df_next.empty:
    st.warning("⚠️ Next-race data not available yet. Showing prediction using latest race data instead.")

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
    """, conn)

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
