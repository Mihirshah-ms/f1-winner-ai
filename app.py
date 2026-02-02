import streamlit as st
import os
import psycopg2

st.title("🏎️ F1 Winner AI — Constructor Strength (Last 24 Races)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_constructor_strength (
    id SERIAL PRIMARY KEY,
    season INT,
    round INT,
    team_id TEXT,
    avg_finish_position_24 FLOAT,
    races_count INT,
    constructor_score FLOAT,
    UNIQUE (season, round, team_id)
);
""")
conn.commit()

# ---------------- FETCH DRIVER RACE RESULTS ----------------
cur.execute("""
SELECT
    r.season,
    r.round,
    rr.team_id,
    rr.position
FROM f1_race_results rr
JOIN f1_races r ON rr.race_id = r.race_id
WHERE rr.position IS NOT NULL
ORDER BY r.season, r.round
""")

rows = cur.fetchall()

# ---------------- BUILD HISTORY ----------------
from collections import defaultdict, deque

history = defaultdict(lambda: deque(maxlen=24))
inserted = 0

for season, rnd, team_id, pos in rows:
    history[team_id].append(pos)

    if len(history[team_id]) >= 5:  # minimum data
        avg_pos = sum(history[team_id]) / len(history[team_id])
        constructor_score = 10 / avg_pos  # lower avg = stronger team

        cur.execute("""
        INSERT INTO f1_constructor_strength
        (season, round, team_id, avg_finish_position_24, races_count, constructor_score)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT (season, round, team_id) DO NOTHING
        """, (
            season, rnd, team_id,
            avg_pos,
            len(history[team_id]),
            constructor_score
        ))

        inserted += cur.rowcount

conn.commit()

st.success("✅ Constructor strength computed")
st.write(f"🏗️ Rows created: {inserted}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT team_id, avg_finish_position_24, constructor_score
FROM f1_constructor_strength
ORDER BY constructor_score DESC
LIMIT 10
""")

st.subheader("Top Constructors (Sample)")
for row in cur.fetchall():
    st.write(row)

cur.close()
conn.close()
