import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI — Qualifying Sync (f1connectapi FULL)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- ENSURE TABLE EXISTS ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_qualifying_results (
    id SERIAL PRIMARY KEY,
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    position INT,
    q1_time TEXT,
    q2_time TEXT,
    q3_time TEXT,
    UNIQUE (season, round, driver_id)
);
""")
conn.commit()

# ---------------- GET COMPLETED ROUNDS ----------------
cur.execute("""
SELECT DISTINCT season, round, race_id
FROM f1_races
WHERE race_date <= CURRENT_DATE
ORDER BY round
""")
rounds = cur.fetchall()

inserted = 0
updated = 0

# ---------------- FETCH & UPSERT QUALIFYING ----------------
for season, rnd, race_id in rounds:
    try:
        url = f"https://f1connectapi.vercel.app/api/{season}/{rnd}/qualy"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        race = data.get("races", {})
        results = race.get("qualyResults", [])

        for res in results:
            driver_id = res.get("driverId")
            position = res.get("gridPosition")
            q1 = res.get("q1")
            q2 = res.get("q2")
            q3 = res.get("q3")

            cur.execute("""
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, position, q1_time, q2_time, q3_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season, round, driver_id) DO UPDATE SET
                position = EXCLUDED.position,
                q1_time = EXCLUDED.q1_time,
                q2_time = EXCLUDED.q2_time,
                q3_time = EXCLUDED.q3_time
            """, (season, rnd, race_id, driver_id, position, q1, q2, q3))

            if cur.rowcount == 1:
                inserted += 1
            else:
                updated += 1

        conn.commit()

    except Exception as e:
        st.warning(f"Skipped {season} round {rnd}: {e}")

st.success("✅ Qualifying sync complete (f1connectapi)")
st.write(f"🆕 Inserted rows: {inserted}")
st.write(f"🔁 Updated rows: {updated}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT season, round, driver_id, position, q1_time, q2_time, q3_time
FROM f1_qualifying_results
ORDER BY season DESC, round DESC, position
LIMIT 12
""")
rows = cur.fetchall()

st.subheader("Sample Qualifying Results (FULL)")
for row in rows:
    st.write(row)

cur.close()
conn.close()
