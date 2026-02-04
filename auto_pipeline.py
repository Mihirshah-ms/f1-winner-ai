import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch

# =========================
# CONFIG
# =========================
BASE_URL = "https://f1api.dev/api"
SEASONS = [2024, 2025]
MAX_ROUNDS = 24
SLEEP_SECONDS = 1.2  # rate limit safety

DB_URL = os.getenv("DATABASE_URL")

# =========================
# DB CONNECT
# =========================
conn = psycopg2.connect(DB_URL)
cur = conn.cursor()

print("🚀 AUTO PIPELINE STARTED")

# =========================
# HELPERS
# =========================
def fetch_json(url):
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        print(f"🌐 API OK → {url}")
        return r.json()
    except Exception as e:
        print(f"❌ API failed: {url} → {e}")
        return None


def safe_int(val):
    try:
        if val in [None, "-", ""]:
            return None
        return int(val)
    except:
        return None


def race_exists(season, rnd):
    cur.execute(
        "SELECT 1 FROM f1_race_results WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


def qualy_exists(season, rnd):
    cur.execute(
        "SELECT 1 FROM f1_qualifying_results WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


def sprint_race_exists(season, rnd):
    cur.execute(
        "SELECT 1 FROM f1_sprint_race_results WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


def sprint_qualy_exists(season, rnd):
    cur.execute(
        "SELECT 1 FROM f1_sprint_qualy_results WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


def fp_exists(table, season, rnd):
    cur.execute(
        f"SELECT 1 FROM {table} WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


# =========================
# SPRINT QUALIFYING
# =========================
def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")
    rows = []

    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):

            if sprint_qualy_exists(season, rnd):
                continue

            url = f"{BASE_URL}/{season}/{rnd}/sprint/qualy"
            data = fetch_json(url)
            time.sleep(SLEEP_SECONDS)

            if not data or "races" not in data:
                continue

            race = data["races"]
            for r in race.get("sprintQualyResults", []):
                rows.append(
                    (
                        season,
                        rnd,
                        race.get("raceId"),
                        r.get("driverId"),
                        r.get("teamId"),
                        safe_int(r.get("gridPosition")),
                    )
                )

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


# =========================
# SPRINT RACE
# =========================
def import_sprint_race():
    print("🏁 Importing sprint race")
    rows = []

    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):

            if sprint_race_exists(season, rnd):
                continue

            url = f"{BASE_URL}/{season}/{rnd}/sprint/race"
            data = fetch_json(url)
            time.sleep(SLEEP_SECONDS)

            if not data or "races" not in data:
                continue

            race = data["races"]
            for r in race.get("sprintRaceResults", []):
                rows.append(
                    (
                        season,
                        rnd,
                        race.get("raceId"),
                        r.get("driverId"),
                        r.get("teamId"),
                        safe_int(r.get("position")),
                        safe_int(r.get("points")),
                    )
                )

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


# =========================
# RUN ORDER
# =========================

import_sprint_qualy()
import_sprint_race()

cur.close()
conn.close()
print("🎉 AUTO PIPELINE COMPLETE")
