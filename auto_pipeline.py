import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch

print("🚀 AUTO PIPELINE STARTED")

# -----------------------
# CONFIG
# -----------------------
DB_URL = os.environ["DATABASE_URL"]
BASE_URL = "https://f1api.dev/api"
SEASONS = [2024, 2025]
MAX_ROUNDS = 24
SLEEP_SECONDS = 0.7   # protects against 429

# -----------------------
# DB CONNECTION
# -----------------------
conn = psycopg2.connect(DB_URL)
conn.autocommit = True
cur = conn.cursor()

# -----------------------
# HELPERS
# -----------------------
def safe_int(v):
    if v in (None, "", "-", "DNS", "DNF", "DSQ"):
        return None
    try:
        return int(v)
    except:
        return None

def fetch_json(url):
    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        print(f"🌐 API OK: {url}")
        return r.json()
    except Exception as e:
        print(f"❌ API failed: {url} → {e}")
        return None

def race_missing(season, rnd, col):
    cur.execute(f"""
        SELECT {col} IS NULL
        FROM f1_races
        WHERE season=%s AND round=%s
    """, (season, rnd))
    r = cur.fetchone()
    return r is not None and r[0]

# -----------------------
# QUALIFYING
# -----------------------
def qualy_exists(season, rnd):
    cur.execute("""
        SELECT 1
        FROM f1_qualifying_results
        WHERE season = %s AND round = %s
        LIMIT 1
    """, (season, rnd))
    return cur.fetchone() is not None
    
def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            if qualy_exists(season, rnd):
                continue

            url = f"{BASE_URL}/{season}/{rnd}/qualy"
            data = fetch_json(url)
            time.sleep(SLEEP_SECONDS)
            if not data or "races" not in data:
                continue

            race = data["races"]
            for r in race.get("qualyResults", []):
                rows.append((
                    season,
                    rnd,
                    race["raceId"],
                    r["driverId"],
                    r["teamId"],
                    r.get("q1"),
                    r.get("q2"),
                    r.get("q3"),
                    safe_int(r.get("gridPosition"))
                ))

    if rows:
        execute_batch(cur, """
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, team_id, q1, q2, q3, grid_position)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
        """, rows)

    print(f"✅ f1_qualifying_results: {len(rows)} rows")

# -----------------------
# PIPELINE RUN
# -----------------------
import_qualy()

print("🎉 AUTO PIPELINE COMPLETE")
