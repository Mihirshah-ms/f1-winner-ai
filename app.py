import streamlit as st
import os
import psycopg2
from collections import defaultdict, deque

st.title("🏁 F1 Winner AI — Driver Recent Form (Last 5 Races)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_driver_recent_form (
    id SERIAL PRIMARY KEY,
    season INT,
    round INT,
    driver_id TEXT,
    avg_finish_5 FLOAT,
    races_count INT,
    UNIQUE (season, round, driver_id)
);
""")
conn.commit()

# ---------------- FETCH RACE RESULTS ----------------
cur.execute("""
SELECT season, round, driver_id, position
FROM f1_race_results
WHERE position IS NOT NULL
ORDER BY season, round
""")

rows = cur.fetchall()

history = defaultdict(lambda: deque(maxlen=5))
inserted = 0

# ---------------- COMPUTE FORM ----------------
for season, rnd, driver_id, pos in rows:
    history[driver_id].append(pos)

    if len(history[driver_id]) >= 3:  # minimum form data
        avg_pos = sum(history[driver_id]) / len(history[driver_id])

        cur.execute("""
        INSERT INTO f1_driver_recent_form
        (season, round, driver_id, avg_finish_5, races_count)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT (season, round, driver_id) DO NOTHING
        """, (
            season,
            rnd,
            driver_id,
            avg_pos,
            len(history[driver_id])
        ))

        inserted += cur.rowcount

conn.commit()

st.success("✅ Driver recent form computed")
st.write(f"🏎️ Rows created: {inserted}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT DISTINCT ON (driver_id)
    driver_id,
    avg_finish_5
FROM f1_driver_recent_form
ORDER BY driver_id, season DESC, round DESC
""")

rows = cur.fetchall()

rows = sorted(rows, key=lambda x: x[1])[:10]

st.subheader("Drivers in Best Recent Form (Latest Only)")
for row in rows:
    st.write(row)

cur.close()
conn.close()
