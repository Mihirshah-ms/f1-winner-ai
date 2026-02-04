import os
import requests
import psycopg2

print("🚀 AUTO PIPELINE STARTED")

# -----------------------------
# Config
# -----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

BASE_URL = "https://f1connectapi.vercel.app/api"

# Backfill ALL
SEASONS = [2024, 2025]
MAX_ROUNDS = 30  # safe upper bound

# -----------------------------
# DB
# -----------------------------
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# -----------------------------
# Helpers
# -----------------------------
def safe_get(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def clean_time(t):
    if not t or t in ["-", "NC", "DNF"]:
        return None
    return t.strip()

# -----------------------------
# ENSURE TABLES
# -----------------------------
cur.execute("""
-- FP1 (base table)
CREATE TABLE IF NOT EXISTS f1_fp1_results (
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    best_time TEXT,
    UNIQUE(season, round, driver_id)
);

-- FP2 and FP3 inherit FP1 structure
CREATE TABLE IF NOT EXISTS f1_fp2_results
(LIKE f1_fp1_results INCLUDING ALL);

CREATE TABLE IF NOT EXISTS f1_fp3_results
(LIKE f1_fp1_results INCLUDING ALL);

-- QUALIFYING
CREATE TABLE IF NOT EXISTS f1_qualifying_results (
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    q1 TEXT,
    q2 TEXT,
    q3 TEXT,
    UNIQUE(season, round, driver_id)
);

-- SPRINT QUALY
CREATE TABLE IF NOT EXISTS f1_sprint_qualy_results (
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    sq1 TEXT,
    sq2 TEXT,
    sq3 TEXT,
    UNIQUE(season, round, driver_id)
);

-- SPRINT RACE
CREATE TABLE IF NOT EXISTS f1_sprint_race_results (
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    UNIQUE(season, round, driver_id)
);

-- RACE
CREATE TABLE IF NOT EXISTS f1_race_results (
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    status TEXT,
    UNIQUE(season, round, driver_id)
);
""")
conn.commit()

# -----------------------------
# FP IMPORT (fp1Results / fp2Results / fp3Results)
# -----------------------------
def import_fp(session, table, key):
    total_rows = 0
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/{session}"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")
            results = race.get(key, [])
            if not results:
                continue

            for i, res in enumerate(results, start=1):
                cur.execute(f"""
                INSERT INTO {table}
                (season, round, race_id, driver_id, team_id, position, best_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    int(rnd),
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    i,
                    clean_time(res.get("time"))
                ))
                total_rows += 1
            conn.commit()
    print(f"✅ {table}: {total_rows} rows")

# -----------------------------
# QUALIFYING (qualyResults)
# -----------------------------
def import_qualy():
    total_rows = 0
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/qualy"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")
            results = race.get("qualyResults", [])
            if not results:
                continue

            for res in results:
                cur.execute("""
                INSERT INTO f1_qualifying_results
                (season, round, race_id, driver_id, team_id, position, q1_time, q2_time, q3_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    int(rnd),
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("gridPosition"),
                    clean_time(res.get("q1")),
                    clean_time(res.get("q2")),
                    clean_time(res.get("q3")),
                ))
                total_rows += 1
            conn.commit()
    print(f"✅ f1_qualifying_results: {total_rows} rows")

# -----------------------------
# SPRINT QUALY (sprintQualyResults)
# -----------------------------
def import_sprint_qualy():
    total_rows = 0
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/sprint/qualy"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")
            results = race.get("sprintQualyResults", [])
            if not results:
                continue

            for res in results:
                cur.execute("""
                INSERT INTO f1_sprint_qualy_results
                (season, round, race_id, driver_id, team_id, position, sq1, sq2, sq3)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    int(rnd),
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("gridPosition"),
                    clean_time(res.get("sq1")),
                    clean_time(res.get("sq2")),
                    clean_time(res.get("sq3")),
                ))
                total_rows += 1
            conn.commit()
    print(f"✅ f1_sprint_qualy_results: {total_rows} rows")

# -----------------------------
# SPRINT RACE (sprintRaceResults)
# -----------------------------
def import_sprint_race():
    total_rows = 0
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/sprint/race"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")
            results = race.get("sprintRaceResults", [])
            if not results:
                continue

            for res in results:
                cur.execute("""
                INSERT INTO f1_sprint_race_results
                (season, round, race_id, driver_id, team_id, position)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    int(rnd),
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("position"),
                ))
                total_rows += 1
            conn.commit()
    print(f"✅ f1_sprint_race_results: {total_rows} rows")

# -----------------------------
# RACE (results with nested driver/team)
# -----------------------------
def import_race():
    total_rows = 0
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/race"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")
            results = race.get("results", [])
            if not results:
                continue

            for res in results:
                driver = res.get("driver", {})
                team = res.get("team", {})
                cur.execute("""
                INSERT INTO f1_race_results
                (season, round, race_id, driver_id, team_id, position, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    int(rnd),
                    race_id,
                    driver.get("driverId"),
                    team.get("teamId"),
                    res.get("position"),
                    "Finished" if not res.get("retired") else "Retired",
                ))
                total_rows += 1
            conn.commit()
    print(f"✅ f1_race_results: {total_rows} rows")

# -----------------------------
# RUN
# -----------------------------
print("🏎️ Importing FP sessions")
import_fp("fp1", "f1_fp1_results", "fp1Results")
import_fp("fp2", "f1_fp2_results", "fp2Results")
import_fp("fp3", "f1_fp3_results", "fp3Results")

print("⏱️ Importing qualifying")
import_qualy()

print("⚡ Importing sprint qualifying")
import_sprint_qualy()

print("🏁 Importing sprint race")
import_sprint_race()

print("🏆 Importing race results")
import_race()

cur.close()
conn.close()

print("🎉 AUTO PIPELINE COMPLETE")
