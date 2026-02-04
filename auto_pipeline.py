import os
import requests
import psycopg2
from psycopg2.extras import execute_batch

print("🚀 AUTO PIPELINE STARTED")

# =========================
# CONFIG
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

SEASONS = [2024, 2025]
BASE_API = "https://f1connectapi.vercel.app/api"

# =========================
# DB
# =========================
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# =========================
# HELPERS
# =========================
def fetch_json(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def get_valid_rounds(season):
    """
    Use calendar endpoint to avoid future rounds
    """
    url = f"{BASE_API}/{season}"
    data = fetch_json(url)
    if not data or "races" not in data:
        return []
    return [int(r["round"]) for r in data["races"]]


# =========================
# FP IMPORTS
# =========================
def import_fp(session, table):
    print(f"🏎️ Importing {session.upper()}")
    rows = []

    for season in SEASONS:
        for rnd in get_valid_rounds(season):
            url = f"{BASE_API}/{season}/{rnd}/{session}"
            data = fetch_json(url)

            if not data or "races" not in data:
                continue

            race = data["races"]
            results_key = f"{session}Results"

            if results_key not in race:
                continue

            for r in race[results_key]:
                rows.append((
                    season,
                    rnd,
                    race.get("raceId"),
                    r.get("driverId"),
                    r.get("teamId"),
                    r.get("time")
                ))

    if not rows:
        print(f"⚠️ No data found for {session.upper()} (normal)")
        return

    execute_batch(cur, f"""
        INSERT INTO {table}
        (season, round, race_id, driver_id, team_id, best_time)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ {table}: {len(rows)} rows")


# =========================
# QUALIFYING
# =========================
def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for season in SEASONS:
        for rnd in get_valid_rounds(season):
            url = f"{BASE_API}/{season}/{rnd}/qualy"
            data = fetch_json(url)

            if not data or "races" not in data:
                continue

            race = data["races"]
            if "qualyResults" not in race:
                continue

            for r in race["qualyResults"]:
                rows.append((
                    season,
                    rnd,
                    race.get("raceId"),
                    r.get("driverId"),
                    r.get("teamId"),
                    r.get("q1"),
                    r.get("q2"),
                    r.get("q3"),
                    r.get("gridPosition")
                ))

    if not rows:
        print("⚠️ No qualifying data yet — skipping")
        return

    execute_batch(cur, """
        INSERT INTO f1_qualifying_results
        (season, round, race_id, driver_id, team_id,
         q1_time, q2_time, q3_time, grid_position)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ f1_qualifying_results: {len(rows)} rows")


# =========================
# SPRINT QUALIFYING
# =========================
def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")
    rows = []

    for season in SEASONS:
        for rnd in get_valid_rounds(season):
            url = f"{BASE_API}/{season}/{rnd}/sprint/qualy"
            data = fetch_json(url)

            if not data or "races" not in data:
                continue

            race = data["races"]
            if "sprintQualyResults" not in race:
                continue

            for r in race["sprintQualyResults"]:
                rows.append((
                    season,
                    rnd,
                    race.get("raceId"),
                    r.get("driverId"),
                    r.get("teamId"),
                    r.get("sq1"),
                    r.get("sq2"),
                    r.get("sq3"),
                    r.get("gridPosition")
                ))

    if not rows:
        print("⚠️ No sprint qualifying yet — skipping")
        return

    execute_batch(cur, """
        INSERT INTO f1_sprint_qualy_results
        (season, round, race_id, driver_id, team_id,
         sq1_time, sq2_time, sq3_time, grid_position)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ f1_sprint_qualy_results: {len(rows)} rows")


# =========================
# SPRINT RACE
# =========================
def import_sprint_race():
    print("🏁 Importing sprint race")
    rows = []

    for season in SEASONS:
        for rnd in get_valid_rounds(season):
            url = f"{BASE_API}/{season}/{rnd}/sprint/race"
            data = fetch_json(url)

            if not data or "races" not in data:
                continue

            race = data["races"]
            if "sprintRaceResults" not in race:
                continue

            for r in race["sprintRaceResults"]:
                rows.append((
                    season,
                    rnd,
                    race.get("raceId"),
                    r.get("driverId"),
                    r.get("teamId"),
                    r.get("position"),
                    r.get("points")
                ))

    if not rows:
        print("⚠️ No sprint race yet — skipping")
        return

    execute_batch(cur, """
        INSERT INTO f1_sprint_race_results
        (season, round, race_id, driver_id, team_id, position, points)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ f1_sprint_race_results: {len(rows)} rows")


# =========================
# RACE RESULTS
# =========================
def import_race():
    print("🏆 Importing race results")
    rows = []

    for season in SEASONS:
        for rnd in get_valid_rounds(season):
            url = f"{BASE_API}/{season}/{rnd}/race"
            data = fetch_json(url)

            if not data or "races" not in data:
                continue

            race = data["races"]
            if "results" not in race:
                continue

            for r in race["results"]:
                rows.append((
                    season,
                    rnd,
                    race.get("raceId"),
                    r["driver"]["driverId"],
                    r["team"]["teamId"],
                    r.get("position"),
                    "Finished"
                ))

    if not rows:
        print("⚠️ No race results yet — skipping")
        return

    execute_batch(cur, """
        INSERT INTO f1_race_results
        (season, round, race_id, driver_id, team_id, position, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ f1_race_results: {len(rows)} rows")


# =========================
# RUN PIPELINE
# =========================
import_fp("fp1", "f1_fp1_results")
import_fp("fp2", "f1_fp2_results")
import_fp("fp3", "f1_fp3_results")
import_qualy()
import_sprint_qualy()
import_sprint_race()
import_race()

print("🎉 AUTO PIPELINE COMPLETE")