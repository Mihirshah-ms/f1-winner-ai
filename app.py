import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI")

# ---------------- DB CONNECTION ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# ---------------- CREATE / MIGRATE RACES TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_races (
    id SERIAL PRIMARY KEY,
    race_id TEXT UNIQUE,
    season INT,
    round INT,
    race_name TEXT,
    race_date DATE,
    race_time TIME,
    qualy_date DATE,
    qualy_time TIME,
    circuit_name TEXT,
    circuit_country TEXT,
    laps INT,
    winner_driver_id TEXT,
    winner_team_id TEXT
);
""")
conn.commit()

# ---------------- FETCH RACES ----------------
races = []
try:
    url = "https://f1api.dev/api/2024"
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()

    for r in data.get("races", []):
        schedule = r.get("schedule", {})
        race = schedule.get("race", {})
        qualy = schedule.get("qualy", {})
        circuit = r.get("circuit", {})
        winner = r.get("winner", {})
        team = r.get("teamWinner", {})

        races.append((
            r.get("raceId"),
            int(data.get("season")),
            r.get("round"),
            r.get("raceName"),
            race.get("date"),
            race.get("time"),
            qualy.get("date"),
            qualy.get("time"),
            circuit.get("circuitName"),
            circuit.get("country"),
            r.get("laps"),
            winner.get("driverId"),
            team.get("teamId")
        ))

except Exception:
    races = []

# ---------------- INSERT / UPDATE RACES ----------------
processed = 0
for race in races:
    cur.execute(
        """
        INSERT INTO f1_races (
            race_id, season, round, race_name,
            race_date, race_time, qualy_date, qualy_time,
            circuit_name, circuit_country, laps,
            winner_driver_id, winner_team_id
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (race_id)
        DO UPDATE SET
            race_name = EXCLUDED.race_name,
            race_date = EXCLUDED.race_date,
            race_time = EXCLUDED.race_time,
            qualy_date = EXCLUDED.qualy_date,
            qualy_time = EXCLUDED.qualy_time,
            circuit_name = EXCLUDED.circuit_name,
            circuit_country = EXCLUDED.circuit_country,
            laps = EXCLUDED.laps,
            winner_driver_id = EXCLUDED.winner_driver_id,
            winner_team_id = EXCLUDED.winner_team_id
        """,
        race
    )
    processed += 1

conn.commit()

st.success("✅ F1 race calendar synced")
st.write(f"🏎️ Races processed this run: {processed}")

# ---------------- DISPLAY RACES ----------------
cur.execute("""
SELECT round, race_name, race_date, circuit_name, winner_driver_id
FROM f1_races
ORDER BY round
""")

rows = cur.fetchall()

st.subheader("2024 F1 Races")
for r in rows:
    st.write(f"Round {r[0]} – {r[1]} | {r[2]} | {r[3]} | Winner: {r[4]}")

cur.close()
conn.close()
