import streamlit as st
import os
import psycopg2

st.title("🏁 F1 Winner AI — Weighted Predictor (24 Races)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- FETCH LAST 24 RACES ----------------
cur.execute("""
SELECT race_id, winner_driver_id
FROM f1_races
WHERE winner_driver_id IS NOT NULL
ORDER BY race_date DESC
LIMIT 24
""")

recent_races = cur.fetchall()

# ---------------- APPLY WEIGHTS ----------------
weights = {}
total_races = len(recent_races)

for index, (_, driver_id) in enumerate(recent_races):
    weight = total_races - index  # 24 → 1
    weights[driver_id] = weights.get(driver_id, 0) + weight

# ---------------- DISPLAY WEIGHTED SCORES ----------------
st.subheader("📊 Weighted Scores (Last 24 Races)")

for driver, score in sorted(weights.items(), key=lambda x: x[1], reverse=True):
    st.write(f"• {driver} → {score}")

# ---------------- FINAL PREDICTION ----------------
if weights:
    predicted_winner = max(weights, key=weights.get)
    st.success(f"🔮 Weighted Prediction for next race: **{predicted_winner}**")
else:
    st.warning("Not enough race data to make a prediction")

cur.close()
conn.close()
