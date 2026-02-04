import os
import requests
import psycopg2
from datetime import datetime

print("🚀 AUTO PIPELINE STARTED")

# =========================
# Database Connection
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# =========================
# Helpers
# =========================
def fetch_json(url):
    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ API failed: {url} → {e}")
        return None


def normalize_races(data):
    """
    f1api.dev ALWAYS returns races as dict
    Normalize to list for safe iteration
    """
    races = data.get("races")
    if not races:
        return []
    if isinstance(races, dict):
        return [races]
    return races


def safe_commit():
    try:
        conn.commit()
    except Exception:
        conn.rollback()


# =========================
# FP IMPORTER
# =========================
def import_fp(session):
    table = f"f1_{session}_results"
    total = 0

    print(f"🏎️ Importing {session.upper()}")

    for year in [2024, 2025]:
        for rnd in range(1, 30):
            url = f"https://f1api.dev/api/{year}/{rnd}/{session}"
            data = fetch_json(url)
            if not data:
                continue

            races = normalize_races(data)
            if not races:
                continue

            race = races[0]
            results_key = f"{session}Results"
            results = race.get(results_key, [])

            if not results:
                continue

            for r in results:
                cur.execute(f"""
                    INSERT INTO {table}
                    (season, round, race_id, driver_id, team_id, best_time)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    year,
                    int(race["round"]),
                    race["raceId"],
                    r["driverId"],
                    r["teamId"],
                    r.get("time")
                ))
                total += 1

            safe_commit()

    print(f"✅ {table}: {total} rows")


# =========================
# QUALIFYING IMPORTER
# =========================
def import_qualy():
    total = 0
    print("⏱️ Importing qualifying")

    for year in [2024, 2025]:
        for rnd in range(1, 30):
            url = f"https://f1api.dev/api/{year}/{rnd}/qualy"
            data = fetch_json(url)
            if not data:
                continue

            races = normalize_races(data)
            if not races:
                continue

            race = races[0]
            results = race.get("qualyResults", [])
            if not results:
                continue

            for r in results:
                cur.execute("""
                    INSERT INTO f1_qualifying_results
                    (season, round, race_id, driver_id, team_id, grid_position, q1_time, q2_time, q3_time)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    year,
                    int(race["round"]),
                    race["raceId"],
                    r["driverId"],
                    r["teamId"],
                    r.get("gridPosition"),
                    r.get("q1"),
                    r.get("q2"),
                    r.get("q3")
                ))
                total += 1

            safe_commit()

    print(f"✅ f1_qualifying_results: {total} rows")


# =========================
# RACE IMPORTER
# =========================
def import_race():
    total = 0
    print("🏆 Importing race results")

    for year in [2024, 2025]:
        for rnd in range(1, 30):
            url = f"https://f1api.dev/api/{year}/{rnd}/race"
            data = fetch_json(url)
            if not data:
                continue

            races = normalize_races(data)
            if not races:
                continue

            race = races[0]
            results = race.get("results", [])
            if not results:
                continue

            for r in results:
                cur.execute("""
                    INSERT INTO f1_race_results
                    (season, round, race_id, driver_id, team_id, position, status)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    year,
                    int(race["round"]),
                    race["raceId"],
                    r["driver"]["driverId"],
                    r["team"]["teamId"],
                    r.get("position"),
                    "Finished" if not r.get("retired") else "DNF"
                ))
                total += 1

            safe_commit()

    print(f"✅ f1_race_results: {total} rows")


# =========================
# SPRINT QUALY
# =========================
def import_sprint_qualy():
    total = 0
    print("⚡ Importing sprint qualifying")

    for year in [2024, 2025]:
        for rnd in range(1, 30):
            url = f"https://f1api.dev/api/{year}/{rnd}/sprint/qualy"
            data = fetch_json(url)
            if not data:
                continue

            races = normalize_races(data)
            if not races:
                continue

            race = races[0]
            results = race.get("sprintQualyResults", [])
            if not results:
                continue

            for r in results:
                cur.execute("""
                    INSERT INTO f1_sprint_qualy_results
                    (season, round, race_id, driver_id, team_id, grid_position, sq1_time, sq2_time, sq3_time)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    year,
                    int(race["round"]),
                    race["raceId"],
                    r["driverId"],
                    r["teamId"],
                    r.get("gridPosition"),
                    r.get("sq1"),
                    r.get("sq2"),
                    r.get("sq3")
                ))
                total += 1

            safe_commit()

    print(f"✅ f1_sprint_qualy_results: {total} rows")


# =========================
# SPRINT RACE
# =========================
def import_sprint_race():
    total = 0
    print("🏁 Importing sprint race")

    for year in [2024, 2025]:
        for rnd in range(1, 30):
            url = f"https://f1api.dev/api/{year}/{rnd}/sprint/race"
            data = fetch_json(url)
            if not data:
                continue

            races = normalize_races(data)
            if not races:
                continue

            race = races[0]
            results = race.get("sprintRaceResults", [])
            if not results:
                continue

            for r in results:
                cur.execute("""
                    INSERT INTO f1_sprint_race_results
                    (season, round, race_id, driver_id, team_id, position)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    year,
                    int(race["round"]),
                    race["raceId"],
                    r["driverId"],
                    r["teamId"],
                    r.get("position")
                ))
                total += 1

            safe_commit()

    print(f"✅ f1_sprint_race_results: {total} rows")


# =========================
# RUN PIPELINE
# =========================
import_fp("fp1")
import_fp("fp2")
import_fp("fp3")

import_qualy()
import_sprint_qualy()
import_sprint_race()
import_race()

cur.close()
conn.close()

print("🎉 AUTO PIPELINE COMPLETE")