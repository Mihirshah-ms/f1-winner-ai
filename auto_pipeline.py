import os
import requests
import psycopg2
import json
from datetime import datetime

print("🚀 AUTO PIPELINE STARTED")

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

BASE_URL = "https://f1connectapi.vercel.app/api"

SEASONS = [2024, 2025]   # backfill all
MAX_ROUNDS = 30          # safe upper bound

# -----------------------------
# Helpers
# -----------------------------
def safe_get(url):
    try:
        r = requests.get(url, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass
    return None

def parse_time(t):
    if not t or t in ["-", "NC", "DNF"]:
        return None
    return t

def ensure_tables():
    cur.execute("""
    CREATE TABLE IF NOT EXISTS f1_fp1_results (
        season INT, round INT, race_id TEXT,
        driver_id TEXT, team_id TEXT,
        position INT,
        best_lap_time TEXT,
        laps JSONB,
        UNIQUE(season, round, driver_id)
    );
    CREATE TABLE IF NOT EXISTS f1_fp2_results (LIKE f1_fp1_results INCLUDING ALL);
    CREATE TABLE IF NOT EXISTS f1_fp3_results (LIKE f1_fp1_results INCLUDING ALL);

    CREATE TABLE IF NOT EXISTS f1_qualifying_results (
        season INT, round INT, race_id TEXT,
        driver_id TEXT, team_id TEXT,
        position INT,
        q1_time TEXT, q2_time TEXT, q3_time TEXT,
        UNIQUE(season, round, driver_id)
    );

    CREATE TABLE IF NOT EXISTS f1_sprint_qualy_results (
        season INT, round INT, race_id TEXT,
        driver_id TEXT, team_id TEXT,
        position INT,
        time TEXT,
        UNIQUE(season, round, driver_id)
    );

    CREATE TABLE IF NOT EXISTS f1_sprint_race_results (
        season INT, round INT, race_id TEXT,
        driver_id TEXT, team_id TEXT,
        position INT,
        status TEXT,
        UNIQUE(season, round, driver_id)
    );

    CREATE TABLE IF NOT EXISTS f1_race_results (
        season INT, round INT, race_id TEXT,
        driver_id TEXT, team_id TEXT,
        position INT,
        status TEXT,
        UNIQUE(season, round, driver_id)
    );
    """)
    conn.commit()

ensure_tables()

# -----------------------------
# Generic session importer
# -----------------------------
def import_fp(session, table):
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/{session}"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")

            for res in race.get("results", []):
                cur.execute(f"""
                INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    rnd,
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("position"),
                    parse_time(res.get("bestLapTime")),
                    json.dumps(res.get("laps", []))
                ))
            conn.commit()

def import_qualy():
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/qualy"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")

            for res in race.get("qualyResults", []):
                cur.execute("""
                INSERT INTO f1_qualifying_results VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    rnd,
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("gridPosition"),
                    parse_time(res.get("q1")),
                    parse_time(res.get("q2")),
                    parse_time(res.get("q3")),
                ))
            conn.commit()

def import_sprint():
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            for kind, table in [
                ("sprint/qualy", "f1_sprint_qualy_results"),
                ("sprint/race", "f1_sprint_race_results"),
            ]:
                url = f"{BASE_URL}/{season}/{rnd}/{kind}"
                data = safe_get(url)
                if not data or "races" not in data:
                    continue

                race = data["races"]
                race_id = race.get("raceId")

                for res in race.get("results", []):
                    cur.execute(f"""
                    INSERT INTO {table} VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                    """, (
                        season,
                        rnd,
                        race_id,
                        res.get("driverId"),
                        res.get("teamId"),
                        res.get("position"),
                        res.get("status")
                    ))
                conn.commit()

def import_race():
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            url = f"{BASE_URL}/{season}/{rnd}/race"
            data = safe_get(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            race_id = race.get("raceId")

            for res in race.get("results", []):
                cur.execute("""
                INSERT INTO f1_race_results VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """, (
                    season,
                    rnd,
                    race_id,
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("position"),
                    res.get("status")
                ))
            conn.commit()

# -----------------------------
# RUN IMPORTS
# -----------------------------
print("🏎️ Importing FP sessions")
import_fp("fp1", "f1_fp1_results")
import_fp("fp2", "f1_fp2_results")
import_fp("fp3", "f1_fp3_results")

print("⏱️ Importing qualifying")
import_qualy()

print("⚡ Importing sprint sessions")
import_sprint()

print("🏁 Importing race results")
import_race()

cur.close()
conn.close()

print("✅ AUTO PIPELINE COMPLETE")