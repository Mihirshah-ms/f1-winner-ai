import os
import time
import requests
import psycopg2

DATABASE_URL = os.environ["DATABASE_URL"]
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

BASE = "https://f1api.dev/api"

print("🚀 AUTO PIPELINE STARTED")

# -------------------------
# Helpers
# -------------------------
def to_int(val):
    if val in (None, "-", "", "NC"):
        return None
    try:
        return int(val)
    except:
        return None


def safe_get(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code == 429:
            print(f"⏳ Rate limited: {url}")
            time.sleep(8)
            return None
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ API failed: {url} → {e}")
        return None


# -------------------------
# Get races needing data
# -------------------------
cur.execute("""
SELECT season, round, race_id
FROM f1_races
ORDER BY season, round
""")

races = cur.fetchall()

# -------------------------
# FP IMPORT
# -------------------------
def import_fp(session, table):
    print(f"🏎️ Importing {session.upper()}")
    rows = 0

    for season, rnd, race_id in races:
        url = f"{BASE}/{season}/{rnd}/{session}"
        data = safe_get(url)
        if not data or "races" not in data:
            continue

        key = f"{session}Results"
        results = data["races"].get(key, [])
        for r in results:
            cur.execute(f"""
            INSERT INTO {table}
            (season, round, race_id, driver_id, team_id, position, time)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                to_int(r.get("fp1Id") or r.get("fp2Id") or r.get("fp3Id")),
                r.get("time")
            ))
            rows += 1

    conn.commit()
    print(f"✅ {table}: {rows} rows")


# -------------------------
# QUALIFYING
# -------------------------
def import_qualy():
    print("⏱️ Importing qualifying")
    rows = 0

    for season, rnd, race_id in races:
        url = f"{BASE}/{season}/{rnd}/qualy"
        data = safe_get(url)
        if not data or "races" not in data:
            continue

        results = data["races"].get("qualyResults", [])
        for r in results:
            cur.execute("""
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, team_id, position)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                to_int(r.get("gridPosition"))
            ))
            rows += 1

    conn.commit()
    print(f"✅ f1_qualifying_results: {rows} rows")


# -------------------------
# SPRINT QUALY
# -------------------------
def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")
    rows = 0

    for season, rnd, race_id in races:
        url = f"{BASE}/{season}/{rnd}/sprint/qualy"
        data = safe_get(url)
        if not data or "races" not in data:
            continue

        results = data["races"].get("sprintQualyResults", [])
        for r in results:
            cur.execute("""
            INSERT INTO f1_sprint_qualy_results
            (season, round, race_id, driver_id, team_id, position)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                to_int(r.get("gridPosition"))
            ))
            rows += 1

    conn.commit()
    print(f"✅ f1_sprint_qualy_results: {rows} rows")


# -------------------------
# SPRINT RACE
# -------------------------
def import_sprint_race():
    print("🏁 Importing sprint race")
    rows = 0

    for season, rnd, race_id in races:
        url = f"{BASE}/{season}/{rnd}/sprint/race"
        data = safe_get(url)
        if not data or "races" not in data:
            continue

        results = data["races"].get("sprintRaceResults", [])
        for r in results:
            cur.execute("""
            INSERT INTO f1_sprint_race_results
            (season, round, race_id, driver_id, team_id, position, points)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                to_int(r.get("position")),
                to_int(r.get("points"))
            ))
            rows += 1

    conn.commit()
    print(f"✅ f1_sprint_race_results: {rows} rows")


# -------------------------
# RACE
# -------------------------
def import_race():
    print("🏆 Importing race results")
    rows = 0

    for season, rnd, race_id in races:
        url = f"{BASE}/{season}/{rnd}/race"
        data = safe_get(url)
        if not data or "races" not in data:
            continue

        results = data["races"].get("results", [])
        for r in results:
            cur.execute("""
            INSERT INTO f1_race_results
            (season, round, race_id, driver_id, team_id, position, points)
            VALUES (%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                to_int(r.get("position")),
                to_int(r.get("points"))
            ))
            rows += 1

    conn.commit()
    print(f"✅ f1_race_results: {rows} rows")


# -------------------------
# RUN
# -------------------------
import_fp("fp1", "f1_fp1_results")
import_fp("fp2", "f1_fp2_results")
import_fp("fp3", "f1_fp3_results")
import_qualy()
import_sprint_qualy()
import_sprint_race()
import_race()

print("🎉 AUTO PIPELINE COMPLETE")
conn.close()