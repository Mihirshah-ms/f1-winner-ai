import os
import time
import requests
import psycopg2

print("🚀 AUTO PIPELINE STARTED (JSON-first, DB-driven)")

# -----------------------------
# Database connection
# -----------------------------
conn = psycopg2.connect(os.environ["DATABASE_URL"])
cur = conn.cursor()

BASE_URL = "https://f1api.dev/api"
REQUEST_DELAY = 1.2  # prevents 429s

# -----------------------------
# Helpers
# -----------------------------
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


def rows_exist(table, season, round_no):
    cur.execute(
        f"""
        SELECT 1
        FROM {table}
        WHERE season = %s AND round = %s
        LIMIT 1
        """,
        (season, round_no),
    )
    return cur.fetchone() is not None


# -----------------------------
# Load races we control from DB
# -----------------------------
cur.execute("""
    SELECT season, round, race_id
    FROM f1_races
    ORDER BY season, round
""")
races = cur.fetchall()

print(f"📋 Loaded {len(races)} races from f1_races")

# ============================================================
# FP IMPORT (fp1 / fp2 / fp3)
# ============================================================
def import_fp(session, table, json_key):
    print(f"🏎️ Importing {session.upper()}")

    inserted = 0

    for season, round_no, race_id in races:
        if rows_exist(table, season, round_no):
            continue

        url = f"{BASE_URL}/{season}/{round_no}/{session}"
        data = safe_get(url)
        time.sleep(REQUEST_DELAY)

        if not data or "races" not in data:
            continue

        results = data["races"].get(json_key)
        if not results:
            continue

        for pos, r in enumerate(results, start=1):
            cur.execute(
                f"""
                INSERT INTO {table}
                (season, round, race_id, driver_id, team_id, position, best_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    season,
                    round_no,
                    race_id,
                    r["driverId"],
                    r["teamId"],
                    pos,
                    r.get("time"),
                ),
            )
            inserted += 1

        conn.commit()

    print(f"✅ {table}: {inserted} rows")


# ============================================================
# QUALIFYING
# ============================================================
def import_qualy():
    print("⏱️ Importing qualifying")

    inserted = 0

    for season, round_no, race_id in races:
        if rows_exist("f1_qualifying_results", season, round_no):
            continue

        url = f"{BASE_URL}/{season}/{round_no}/qualy"
        data = safe_get(url)
        time.sleep(REQUEST_DELAY)

        if not data or "races" not in data:
            continue

        results = data["races"].get("qualyResults")
        if not results:
            continue

        for r in results:
            cur.execute(
                """
                INSERT INTO f1_qualifying_results
                (season, round, race_id, driver_id, team_id, q1, q2, q3, grid_position)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    season,
                    round_no,
                    race_id,
                    r["driverId"],
                    r["teamId"],
                    r.get("q1"),
                    r.get("q2"),
                    r.get("q3"),
                    r.get("gridPosition"),
                ),
            )
            inserted += 1

        conn.commit()

    print(f"✅ f1_qualifying_results: {inserted} rows")


# ============================================================
# RACE RESULTS
# ============================================================
def import_race():
    print("🏆 Importing race results")

    inserted = 0

    for season, round_no, race_id in races:
        if rows_exist("f1_race_results", season, round_no):
            continue

        url = f"{BASE_URL}/{season}/{round_no}/race"
        data = safe_get(url)
        time.sleep(REQUEST_DELAY)

        if not data or "races" not in data:
            continue

        results = data["races"].get("results")
        if not results:
            continue

        for r in results:
            cur.execute(
                """
                INSERT INTO f1_race_results
                (season, round, race_id, driver_id, team_id, position, points, grid, time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
                """,
                (
                    season,
                    round_no,
                    race_id,
                    r["driver"]["driverId"],
                    r["team"]["teamId"],
                    r["position"],
                    r.get("points", 0),
                    r.get("grid"),
                    r.get("time"),
                ),
            )
            inserted += 1

        conn.commit()

    print(f"✅ f1_race_results: {inserted} rows")


# ============================================================
# RUN PIPELINE
# ============================================================
import_fp("fp1", "f1_fp1_results", "fp1Results")
import_fp("fp2", "f1_fp2_results", "fp2Results")
import_fp("fp3", "f1_fp3_results", "fp3Results")
import_qualy()
import_race()

print("🎉 AUTO PIPELINE COMPLETE")
cur.close()
conn.close()