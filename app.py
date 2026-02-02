import streamlit as st
import os
import psycopg2

st.title("🏁 F1 Winner AI — Baseline Predictor")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- FETCH WIN COUNTS ----------------
cur.execute("""
SELECT winner_driver_id, COUNT(*) AS wins
FROM f1_races
WHERE winner_driver_id IS NOT NULL
GROUP BY winner_driver_id
ORDER BY wins DESC
""")

rows = cur.fetchall()

st.subheader("🏆 2024 Win Count")
for r in rows:
    st.write(f"• {r[0]} — {r[1]} wins")

# ---------------- BASELINE PREDICTION ----------------
if rows:
    predicted_winner = rows[0][0]
    st.success(f"🔮 Baseline Prediction for next race: **{predicted_winner}**")
else:
    st.warning("Not enough data to make a prediction")

cur.close()
conn.close()
