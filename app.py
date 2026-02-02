import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI — Qualifying Sync (FULL)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE QUALIFYING TABLE ----------------
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

# ---------------- FETCH & INSERT QUALIFYING ----------------
for season, rnd, race_id in rounds:
    try:
        url = f"https://f1api.dev/api/{season}/{rnd}/qualy"
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        data = response.json()

        race = data.get("races", {})
        results = race.get("qualifyingResults", [])

        for r in results:
            cur.execute(
                """
                INSERT INTO f1_qualifying_results
                (season, round, race_id, driver_id, position, q1_time, q2_time, q3_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (season, round, driver_id) DO NOTHING
                """,
                (
                    season,
                    rnd,
                    race_id,
                    r.get("driverId"),
                    r.get("position"),
                    r.get("q1"),
                    r.get("q2"),
                    r.get("q3")
                )
            )
            inserted += cur.rowcount

        conn.commit()

    except Exception:
        continue

st.success("✅ Qualifying data synced successfully")
st.write(f"🏎️ New qualifying rows added: {inserted}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT season, round, driver_id, position, q3_time
FROM f1_qualifying_results
ORDER BY season DESC, round DESC, position
LIMIT 10
""")

rows = cur.fetchall()

st.subheader("Sample Qualifying Results")
for r in rows:
    st.write(r)

cur.close()
conn.close()
