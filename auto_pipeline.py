import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_values

print("🚀 AUTO PIPELINE STARTED")

# -------------------------------------------------
# Config
# -------------------------------------------------
BASE_API = "https://f1api.dev/api"
SLEEP_SECONDS = 1.2  # rate-limit safe
SEASONS = [2024, 2025]

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# -------------------------------------------------
# Helpers
# -------------------------------------------------
def safe_get(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            print(f"❌ API failed: {url} → {r.status_code}")
            return None
        return r.json()
    except Exception as e:
        print(f"❌ Request error: {url} → {e}")
        return None


def race_needs_data(table, season, rnd):
    cur.execute(
        f"""
        SELECT 1
        FROM {table}
        WHERE season = %s AND round = %s
        LIMIT 1
        """,
        (season, rnd),
    )
    return cur.fetchone() is None


def get_races():
    cur.execute("""
        SELECT race_id, season, round
        FROM f1_races
        ORDER BY season, round
    """)
    return cur.fetchall()

# -------------------------------------------------
# FP IMPORT
# -------------------------------------------------
def import_fp(session, table):
    print(f"🏎️ Importing {session.upper()}")
    rows = []

    for race_id, season, rnd in get_races():
        if not race_needs_data(table, season, rnd):
            continue

        url = f"{BASE_API}/{season}/{rnd}/{session}"
        data = safe_get(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        results_key = f"{session}Results"
        results = data["races"].get(results_key)
        if not results:
            continue

        for r in results:
            rows.append((
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                r.get("time"),
            ))

    if rows:
        execute_values(
            cur,
            f"""
            INSERT INTO {table}
            (season, round, race_id, driver_id, team_id, best_time)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ {table}: {len(rows)} rows")

# -------------------------------------------------
# QUALIFYING
# -------------------------------------------------
def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for race_id, season, rnd in get_races():
        if not race_needs_data("f1_qualifying_results", season, rnd):
            continue

        url = f"{BASE_API}/{season}/{rnd}/qualy"
        data = safe_get(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        results = data["races"].get("qualyResults")
        if not results:
            continue

        for r in results:
            rows.append((
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                r.get("q1"),
                r.get("q2"),
                r.get("q3"),
                r.get("gridPosition"),
            ))

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, team_id, q1, q2, q3, grid_position)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_qualifying_results: {len(rows)} rows")

# -------------------------------------------------
# RACE RESULTS
# -------------------------------------------------
def import_race():
    print("🏁 Importing race results")
    rows = []

    for race_id, season, rnd in get_races():
        if not race_needs_data("f1_race_results", season, rnd):
            continue

        url = f"{BASE_API}/{season}/{rnd}/race"
        data = safe_get(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        results = data["races"].get("results")
        if not results:
            continue

        for r in results:
            rows.append((
                season,
                rnd,
                race_id,
                r["driver"]["driverId"],
                r["team"]["teamId"],
                r.get("position"),
                r.get("points"),
            ))

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO f1_race_results
            (season, round, race_id, driver_id, team_id, position, points)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_race_results: {len(rows)} rows")

# -------------------------------------------------
# SPRINT QUALIFYING
# -------------------------------------------------
def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")
    rows = []

    for race_id, season, rnd in get_races():
        if not race_needs_data("f1_sprint_qualy_results", season, rnd):
            continue

        url = f"{BASE_API}/{season}/{rnd}/sprint/qualy"
        data = safe_get(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        results = data["races"].get("sprintQualyResults")
        if not results:
            continue

        for r in results:
            rows.append((
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                r.get("gridPosition"),
            ))

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO f1_sprint_qualy_results
            (season, round, race_id, driver_id, team_id, grid_position)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_sprint_qualy_results: {len(rows)} rows")

# -------------------------------------------------
# SPRINT RACE
# -------------------------------------------------
def import_sprint_race():
    print("🏁 Importing sprint race")
    rows = []

    for race_id, season, rnd in get_races():
        if not race_needs_data("f1_sprint_race_results", season, rnd):
            continue

        url = f"{BASE_API}/{season}/{rnd}/sprint/race"
        data = safe_get(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        results = data["races"].get("sprintRaceResults")
        if not results:
            continue

        for r in results:
            rows.append((
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                r.get("position"),
                r.get("points"),
            ))

    if rows:
        execute_values(
            cur,
            """
            INSERT INTO f1_sprint_race_results
            (season, round, race_id, driver_id, team_id, position, points)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_sprint_race_results: {len(rows)} rows")

# -------------------------------------------------
# RUN PIPELINE
# -------------------------------------------------
import_fp("fp1", "f1_fp1_results")
import_fp("fp2", "f1_fp2_results")
import_fp("fp3", "f1_fp3_results")
import_qualy()
import_sprint_qualy()
import_sprint_race()
import_race()

cur.close()
conn.close()

print("🎉 AUTO PIPELINE COMPLETE")