import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch

# ============================================================
# CONFIG (2026 ONLY)
# ============================================================
BASE_URL = "https://f1api.dev/api"
SEASON = 2026
MAX_ROUNDS = 24
SLEEP_SECONDS = 1.2  # rate-limit safe

DB_URL = os.getenv("DATABASE_URL")

# ============================================================
# DB CONNECT
# ============================================================
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

print("🚀 AUTO PIPELINE STARTED (2026 ONLY)")

# ============================================================
# HELPERS
# ============================================================
def fetch_json(url):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        print(f"🌐 API OK → {url}")
        return r.json()
    except Exception as e:
        print(f"❌ API failed: {url} → {e}")
        return None


def safe_int(val):
    if val in [None, "-", ""]:
        return None
    try:
        return int(val)
    except:
        return None


def exists(table, season, rnd):
    cur.execute(
        f"SELECT 1 FROM {table} WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


# ============================================================
# RACE CALENDAR (f1_races)
# ============================================================
def import_2026_calendar():
    print("📅 Importing 2026 race calendar")
    rows = []

    season = 2026

    for rnd in range(1, MAX_ROUNDS + 1):
        url = f"{BASE_URL}/{season}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data:
            continue

        race = data["race"][0]

        schedule = race.get("schedule", {})
        race_sched = schedule.get("race", {})
        qualy_sched = schedule.get("qualy", {})
        circuit = race.get("circuit", {})

        rows.append((
            race.get("raceId"),
            season,
            race.get("round"),
            race.get("raceName"),
            race_sched.get("date"),
            race_sched.get("time"),
            qualy_sched.get("date"),
            qualy_sched.get("time"),
            circuit.get("circuitName"),
            circuit.get("country"),
            race.get("laps"),
        ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_races
            (race_id, season, round, race_name,
             race_date, race_time,
             qualy_date, qualy_time,
             circuit_name, circuit_country, laps)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season, round) DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_races (2026 calendar): {len(rows)} rows")

# ============================================================
# FP SESSIONS (FP1 / FP2 / FP3)
# ============================================================
def import_fp(session, table, key):
    print(f"🏎️ Importing {session.upper()}")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists(table, SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}/{session}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        race = data["races"]
        for r in race.get(key, []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r.get("driverId"),
                r.get("teamId"),
                r.get("time"),
            ))

    if rows:
        execute_batch(
            cur,
            f"""
            INSERT INTO {table}
            (season, round, race_id, driver_id, team_id, best_time)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ {table}: {len(rows)} rows")


# ============================================================
# QUALIFYING
# ============================================================
def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_qualifying_results", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}/qualy"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        race = data["races"]
        for r in race.get("qualyResults", []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r.get("driverId"),
                r.get("teamId"),
                r.get("q1"),
                r.get("q2"),
                r.get("q3"),
                safe_int(r.get("gridPosition")),
            ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, team_id, q1, q2, q3, grid_position)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_qualifying_results: {len(rows)} rows")


# ============================================================
# SPRINT QUALIFYING
# ============================================================
def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_sprint_qualy_results", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}/sprint/qualy"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        race = data["races"]
        for r in race.get("sprintQualyResults", []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r.get("driverId"),
                r.get("teamId"),
                safe_int(r.get("gridPosition")),
            ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_sprint_qualy_results
            (season, round, race_id, driver_id, team_id, grid_position)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_sprint_qualy_results: {len(rows)} rows")


# ============================================================
# SPRINT RACE
# ============================================================
def import_sprint_race():
    print("🏁 Importing sprint race")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_sprint_race_results", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}/sprint/race"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        race = data["races"]
        for r in race.get("sprintRaceResults", []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r.get("driverId"),
                r.get("teamId"),
                safe_int(r.get("position")),
                safe_int(r.get("points")),
            ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_sprint_race_results
            (season, round, race_id, driver_id, team_id, position, points)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_sprint_race_results: {len(rows)} rows")


# ============================================================
# RACE RESULTS
# ============================================================
def import_race_results():
    print("🏆 Importing race results")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_race_results", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "races" not in data:
            continue

        race = data["races"]
        for r in race.get("results", []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r["driver"]["driverId"],
                r["team"]["teamId"],
                safe_int(r.get("position")),
                safe_int(r.get("grid")),
                safe_int(r.get("points")),
                r.get("time"),
                r.get("retired"),
            ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_race_results
            (season, round, race_id, driver_id, team_id,
             position, grid, points, race_time, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_race_results: {len(rows)} rows")


# ============================================================
# RUN ORDER
# ============================================================
import_2026_calendar()
import_fp("fp1", "f1_fp1_results", "fp1Results")
import_fp("fp2", "f1_fp2_results", "fp2Results")
import_fp("fp3", "f1_fp3_results", "fp3Results")
import_qualy()
import_sprint_qualy()
import_sprint_race()
import_race_results()

cur.close()
conn.close()
print("🎉 AUTO PIPELINE COMPLETE (2026)")
