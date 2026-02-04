import streamlit as st
import os
import psycopg2
import pandas as pd
import pickle

st.set_page_config(page_title="F1 Winner AI", layout="wide")
st.title("🏁 F1 Winner AI")
st.caption("Fully automated race & championship prediction")

DATABASE_URL = os.getenv("DATABASE_URL")

# ---------------- LOAD MODEL ----------------
cur = conn.cursor()
cur.execute("""
SELECT model_blob
FROM ml_models
WHERE model_name = 'f1_winner_model'
ORDER BY trained_at DESC
LIMIT 1;
""")

row = cur.fetchone()

if not row:
    st.error("❌ Model not trained yet. Wait for cron run.")
    st.stop()

model = pickle.loads(row[0])

# ---------------- DB ----------------
conn = psycopg2.connect(DATABASE_URL)

# ---------------- RACE INFO ----------------
race = pd.read_sql("""
SELECT season, round, race_name, race_date,
       circuit_name, circuit_country
FROM f1_races
ORDER BY season DESC, round DESC
LIMIT 1;
""", conn).iloc[0]

date = pd.to_datetime(race["race_date"]).strftime("%d %B %Y") if race["race_date"] else "TBA"

# ---------------- PREDICTION ----------------
df = pd.read_sql(f"""
SELECT
  q.driver_id,
  q.qualy_score,
  d.avg_finish_5,
  c.avg_team_finish_24
FROM f1_qualifying_features q
LEFT JOIN f1_driver_recent_form d
  ON q.driver_id = d.driver_id AND q.round = d.round
LEFT JOIN f1_constructor_strength c
  ON q.round = c.round
WHERE q.season={race['season']} AND q.round={race['round']};
""", conn)

X = df[["qualy_score","avg_finish_5","avg_team_finish_24"]]
df["win_prob"] = model.predict_proba(X)[:,1]
df = df.sort_values("win_prob",ascending=False)

# ---------------- UI ----------------
st.subheader("🏎️ Next Race")
st.markdown(f"""
**{race['race_name']}**  
📅 {date}  
📍 {race['circuit_name']}, {race['circuit_country']}
""")

st.metric("🏆 Predicted Winner", df.iloc[0]["driver_id"].upper())
st.metric("📈 Win Probability", f"{df.iloc[0]['win_prob']*100:.1f}%")

st.subheader("🥉 Podium Probabilities")
st.table(
    df.head(3)[["driver_id","win_prob"]]
    .assign(win_prob=lambda x:(x.win_prob*100).round(2))
    .rename(columns={"driver_id":"Driver","win_prob":"Win %"})
)

# ---------------- CHAMPIONSHIP ----------------
st.subheader("🏆 Championship Prediction")

champ = pd.read_sql("""
SELECT driver_id, current_points,
       expected_future_points,
       projected_total_points,
       championship_probability
FROM f1_championship_projection
ORDER BY championship_probability DESC;
""", conn)

champ["Championship %"] = (champ["championship_probability"]*100).round(2)

st.dataframe(
    champ.rename(columns={
        "driver_id":"Driver",
        "current_points":"Current Pts",
        "expected_future_points":"Expected Pts",
        "projected_total_points":"Projected Total"
    }),
    use_container_width=True
)

conn.close()
