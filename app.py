import streamlit as st
import os
import psycopg2

st.title("🏁 F1 Winner AI — Qualifying Features (Balanced & Clean)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE FEATURES TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_qualifying_features (
    id SERIAL PRIMARY KEY,
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    grid_position INT,
    reached_q1 INT,
    reached_q2 INT,
    reached_q3 INT,
    q1_seconds FLOAT,
    q2_seconds FLOAT,
    q3_seconds FLOAT,
    qualy_score FLOAT,
    UNIQUE (season, round, driver_id)
);
""")
conn.commit()

# ---------------- HELPER ----------------
def time_to_seconds(t):
    if not t:
        return None

    parts = t.split(":")

    try:
        if len(parts) == 2:
            # Format: M:SS.mmm
            minutes = int(parts[0])
            seconds = float(parts[1])
        elif len(parts) == 3:
            # Format: M:SS:mmm
            minutes = int(parts[0])
            seconds = int(parts[1]) + int(parts[2]) / 1000
        else:
            return None

        return minutes * 60 + seconds

    except Exception:
        return None

# ---------------- BUILD FEATURES ----------------
cur.execute("""
SELECT season, round, race_id, driver_id, position, q1_time, q2_time, q3_time
FROM f1_qualifying_results
WHERE position IS NOT NULL
""")

rows = cur.fetchall()
inserted = 0

for season, rnd, race_id, driver, pos, q1, q2, q3 in rows:
    q1_s = time_to_seconds(q1)
    q2_s = time_to_seconds(q2)
    q3_s = time_to_seconds(q3)

    reached_q1 = 1
    reached_q2 = 1 if q2_s is not None else 0
    reached_q3 = 1 if q3_s is not None else 0

    qualy_score = (
        (1 / pos) * 6
        + reached_q3 * 3
        + reached_q2 * 2
        + reached_q1 * 1
    )

    cur.execute("""
    INSERT INTO f1_qualifying_features
    (season, round, race_id, driver_id, grid_position,
     reached_q1, reached_q2, reached_q3,
     q1_seconds, q2_seconds, q3_seconds, qualy_score)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (season, round, driver_id) DO NOTHING
    """, (
        season, rnd, race_id, driver, pos,
        reached_q1, reached_q2, reached_q3,
        q1_s, q2_s, q3_s, qualy_score
    ))

    inserted += cur.rowcount

conn.commit()

st.success("✅ Qualifying features generated (Balanced)")
st.write(f"📊 Feature rows created: {inserted}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT driver_id, grid_position, reached_q3, qualy_score
FROM f1_qualifying_features
ORDER BY qualy_score DESC
LIMIT 12
""")

st.subheader("Top Qualifying Scores (Sample)")
for row in cur.fetchall():
    st.write(row)

cur.close()
conn.close()
