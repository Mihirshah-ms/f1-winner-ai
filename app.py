import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI — Race Results Sync (FULL)")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_race_results (
    id SERIAL PRIMARY KEY,
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    status TEXT,
    UNIQUE (season, round, driver_id)
);
""")
conn.commit()

# ---------------- GET COMPLETED RACES ----------------
cur.execute("""
SELECT season, round, race_id
FROM f1_races
WHERE race_date <= CURRENT_DATE
ORDER BY season, round
""")

races = cur.fetchall()
inserted = 0

# ---------------- FETCH & INSERT RESULTS ----------------
for season, rnd, race_id in races:
    try:
        url = f"https://f1api.dev/api/{season}/{rnd}/race"
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

        race = data.get("races", {})
        results = race.get("results", [])

        for res in results:
            raw_pos = res.get("position")
            position = int(raw_pos) if raw_pos and str(raw_pos).isdigit() else None

            driver = res.get("driver", {})
            team = res.get("team", {})

            driver_id = driver.get("driverId")
            team_id = team.get("teamId")

            cur.execute("""
            INSERT INTO f1_race_results
            (season, round, race_id, driver_id, team_id, position, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season, round, driver_id) DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                driver_id,
                team_id,
                position,
                res.get("status")
            ))


            inserted += cur.rowcount

        conn.commit()

    except Exception as e:
        conn.rollback()
        st.warning(f"Skipped {season} round {rnd}: {e}")

cur.execute("DELETE FROM f1_race_results WHERE driver_id IS NULL")
conn.commit()

st.success("✅ Race results synced")
st.write(f"🏁 Race result rows added: {inserted}")

# ---------------- DISPLAY SAMPLE ----------------
cur.execute("""
SELECT season, round, driver_id, team_id, position
FROM f1_race_results
ORDER BY season DESC, round DESC, position
LIMIT 12
""")

st.subheader("Sample Race Results")
for row in cur.fetchall():
    st.write(row)

cur.close()
conn.close()
