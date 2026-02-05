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
# RUN ORDER
# ============================================================
import_race_calendar()

cur.close()
conn.close()
print("🎉 AUTO PIPELINE COMPLETE (2026)")
