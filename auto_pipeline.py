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
SLEEP_SECONDS = 1.2

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


def safe_str(val):
    return val if val not in ["", "-"] else None


def exists(table, season, rnd):
    cur.execute(
        f"SELECT 1 FROM {table} WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


# ============================================================
# RACE CALENDAR
# ============================================================
def import_race_calendar():
    print("📅 Importing race calendar (2026)")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_races", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data or not data["race"]:
            continue

        race = data["race"][0]
        schedule = race.get("schedule", {})
        circuit = race.get("circuit", {})

        rows.append((
            race.get("raceId"),
            SEASON,
            rnd,
            race.get("raceName"),
            schedule.get("race", {}).get("date"),
            schedule.get("race", {}).get("time"),
            schedule.get("qualy", {}).get("date"),
            schedule.get("qualy", {}).get("time"),
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

    print(f"✅ f1_races: {len(rows)} rows")


# ============================================================
# FP / QUALY / SPRINT / RACE RESULTS
# ============================================================
def import_session(
    label, table, session_key, fields
):
    print(f"🏎️ Importing {label}")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists(table, SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data or not data["race"]:
            continue

        race = data["race"][0]
        session_rows = race.get(session_key, [])

        for r in session_rows:
            row = [SEASON, rnd, race.get("raceId")]
            for f in fields:
                row.append(
                    safe_int(r.get(f))
                    if "position" in f or "grid" in f or "points" in f
                    else safe_str(r.get(f))
                )
            rows.append(tuple(row))

    if rows:
        placeholders = ",".join(["%s"] * len(rows[0]))
        execute_batch(
            cur,
            f"""
            INSERT INTO {table}
            VALUES ({placeholders})
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ {table}: {len(rows)} rows")


# ============================================================
# RUN ORDER (2026)
# ============================================================
import_race_calendar()

# NOTE:
# FP / Qualy / Sprint / Race results
# will naturally be EMPTY until sessions happen.
# This is expected and SAFE.

cur.close()
conn.close()
print("🎉 AUTO PIPELINE COMPLETE (2026 ONLY)")