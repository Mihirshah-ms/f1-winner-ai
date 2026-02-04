import sys
import time
import requests
import psycopg2
from psycopg2.extras import execute_values
import os

# ---------------- CONFIG ----------------
BASE_URL = "https://f1api.dev/api"
SEASON = int(sys.argv[1])  # 2024 or 2025
SLEEP = 1.2  # rate-limit safety

DATABASE_URL = os.environ["DATABASE_URL"]

# ----------------------------------------

def log(msg):
    print(msg, flush=True)

def fetch(url):
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        return None
    return r.json()

def connect():
    return psycopg2.connect(DATABASE_URL)

def backfill_races(cur):
    log("🏁 Backfilling races metadata")
    url = f"{BASE_URL}/{SEASON}/races"
    data = fetch(url)
    if not data:
        log("❌ Failed to fetch races list")
        return []

    races = data["races"]
    rounds = []

    for r in races:
        cur.execute("""
        INSERT INTO f1_races (
            race_id, season, round, race_name,
            race_date, race_time,
            circuit_name, circuit_country
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (race_id) DO NOTHING
        """, (
            r["raceId"],
            SEASON,
            int(r["round"]),
            r["raceName"],
            r.get("date"),
            r.get("time"),
            r["circuit"]["circuitName"],
            r["circuit"]["country"]
        ))
        rounds.append(int(r["round"]))

    log(f"✅ Races loaded: {len(rounds)}")
    return rounds

def backfill_fp(cur, round_no, session):
    url = f"{BASE_URL}/{SEASON}/{round_no}/{session}"
    data = fetch(url)
    if not data or "races" not in data:
        return 0

    key = f"{session}Results"
    if key not in data["races"]:
        return 0

    rows = []
    for r in data["races"][key]:
        rows.append((
            SEASON,
            round_no,
            data["races"]["raceId"],
            r["driverId"],
            r["teamId"],
            r["time"]
        ))

    execute_values(cur, f"""
        INSERT INTO f1_{session}_results
        (season, round, race_id, driver_id, team_id, best_time)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)

    return len(rows)

def backfill_qualy(cur, round_no):
    url = f"{BASE_URL}/{SEASON}/{round_no}/qualy"
    data = fetch(url)
    if not data or "qualyResults" not in data["races"]:
        return 0

    rows = []
    for q in data["races"]["qualyResults"]:
        rows.append((
            SEASON,
            round_no,
            data["races"]["raceId"],
            q["driverId"],
            q["teamId"],
            q.get("q1"),
            q.get("q2"),
            q.get("q3"),
            q["gridPosition"]
        ))

    execute_values(cur, """
        INSERT INTO f1_qualifying_results
        (season, round, race_id, driver_id, team_id, q1_time, q2_time, q3_time, grid_position)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)

    return len(rows)

def backfill_race(cur, round_no):
    url = f"{BASE_URL}/{SEASON}/{round_no}/race"
    data = fetch(url)
    if not data or "results" not in data["races"]:
        return 0

    rows = []
    for r in data["races"]["results"]:
        pos = r.get("position")
        if isinstance(pos, str):
            pos = None

        rows.append((
            SEASON,
            round_no,
            data["races"]["raceId"],
            r["driver"]["driverId"],
            r["team"]["teamId"],
            pos,
            r.get("retired")
        ))

    execute_values(cur, """
        INSERT INTO f1_race_results
        (season, round, race_id, driver_id, team_id, position, status)
        VALUES %s
        ON CONFLICT DO NOTHING
    """, rows)

    return len(rows)

# ---------------- MAIN ----------------

if __name__ == "__main__":
    log(f"🚀 BACKFILL STARTED — Season {SEASON}")

    conn = connect()
    cur = conn.cursor()

    rounds = backfill_races(cur)
    conn.commit()

    for rnd in rounds:
        log(f"🔁 Round {rnd}")

        for fp in ["fp1", "fp2", "fp3"]:
            n = backfill_fp(cur, rnd, fp)
            log(f"   {fp.upper()}: {n}")

        q = backfill_qualy(cur, rnd)
        log(f"   QUALY: {q}")

        r = backfill_race(cur, rnd)
        log(f"   RACE: {r}")

        conn.commit()
        time.sleep(SLEEP)

    log("🎉 BACKFILL COMPLETE")
    cur.close()
    conn.close()