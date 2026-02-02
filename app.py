import streamlit as st
import os
import psycopg2

st.title("🤖 F1 Winner AI — Build Training Dataset")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE TRAINING TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_training_data (
    id SERIAL PRIMARY KEY,
    season INT,
    round INT,
    driver_id TEXT,
    qualy_score FLOAT,
    constructor_score FLOAT,
    avg_driver_form FLOAT,
    winner INT,
    UNIQUE (season, round, driver_id)
);
""")
conn.commit()

# ---------------- BUILD DATASET ----------------
cur.execute("""
SELECT
    rr.season,
    rr.round,
    rr.driver_id,
    qf.qualy_score,
    cs.constructor_score,
    df.avg_finish_5,
    CASE WHEN rr.position = 1 THEN 1 ELSE 0 END AS winner
FROM f1_race_results rr
JOIN f1_qualifying_features qf
    ON rr.season = qf.season
   AND rr.round = qf.round
   AND rr.driver_id = qf.driver_id
JOIN f1_constructor_strength cs
    ON rr.season = cs.season
   AND rr.round = cs.round
   AND rr.team_id = cs.team_id
JOIN f1_driver_recent_form df
    ON rr.season = df.season
   AND rr.round = df.round
   AND rr.driver_id = df.driver_id
WHERE rr.position IS NOT NULL
""")

rows = cur.fetchall()
inserted = 0

for row in rows:
    cur.execute("""
    INSERT INTO f1_training_data
    (season, round, driver_id, qualy_score, constructor_score, avg_driver_form, winner)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (season, round, driver_id) DO NOTHING
    """, row)

    inserted += cur.rowcount

conn.commit()

st.success("✅ Training dataset built")
st.write(f"📊 Rows created: {inserted}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT driver_id, qualy_score, constructor_score, avg_driver_form, winner
FROM f1_training_data
ORDER BY season DESC, round DESC, winner DESC
LIMIT 12
""")

st.subheader("Sample Training Rows")
for row in cur.fetchall():
    st.write(row)

cur.close()
conn.close()
