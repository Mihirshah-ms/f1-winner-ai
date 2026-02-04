import os
import psycopg2
import requests

DATABASE_URL = os.environ.get("DATABASE_URL")

SEASONS = [2024, 2025]
ROUNDS = range(1, 25)
BASE_URL = "https://f1connectapi.vercel.app/api"

print("🚀 AUTO PIPELINE STARTED")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


# ---------------------------------------------------
# Helper
# ---------------------------------------------------
def fetch_json(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


# ---------------------------------------------------
# FP IMPORTER
# ---------------------------------------------------
def import_fp(session, table, result_key):
    print(f"🏎️ Importing {session.upper()}")

    inserted = 0

    for season in SEASONS:
        for rnd in ROUNDS:
            url = f"{BASE_URL}/{season}/{rnd}/{session}?limit=30"
            data = fetch_json(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            results = race.get(result_key, [])
            if not results:
                continue

            # Sort by time to derive position
            clean = [r for r in results if r.get("time")]
            clean.sort(key=lambda x: x["time"])

            for pos, res in enumerate(clean, start=1):
                try:
                    cur.execute(
                        f"""
                        INSERT INTO {table}
                        (season, round, race_id, driver_id, team_id, position, best_time)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            season,
                            int(race["round"]),
                            race["raceId"],
                            res["driverId"],
                            res["teamId"],
                            pos,
                            res["time"],
                        ),
                    )
                    inserted += 1
                except Exception:
                    conn.rollback()
                    continue

            conn.commit()

    print(f"✅ {table}: {inserted} rows")


# ---------------------------------------------------
# QUALIFYING
# ---------------------------------------------------
def import_qualy():
    print("⏱️ Importing qualifying")

    inserted = 0

    for season in SEASONS:
        for rnd in ROUNDS:
            url = f"{BASE_URL}/{season}/{rnd}/qualy?limit=30"
            data = fetch_json(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            results = race.get("qualyResults", [])
            if not results:
                continue

            for res in results:
                try:
                    cur.execute(
                        """
                        INSERT INTO f1_qualifying_results
                        (season, round, race_id, driver_id, team_id, grid_position, q1, q2, q3)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT (season, round, driver_id) DO NOTHING
                        """,
                        (
                            season,
                            int(race["round"]),
                            race["raceId"],
                            res["driverId"],
                            res["teamId"],
                            res.get("gridPosition"),
                            res.get("q1"),
                            res.get("q2"),
                            res.get("q3"),
                        ),
                    )
                    inserted += 1
                except Exception:
                    conn.rollback()
                    continue

            conn.commit()

    print(f"✅ f1_qualifying_results: {inserted} rows")


# ---------------------------------------------------
# SPRINT QUALY
# ---------------------------------------------------
def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")

    inserted = 0

    for season in SEASONS:
        for rnd in ROUNDS:
            url = f"{BASE_URL}/{season}/{rnd}/sprint/qualy?limit=30"
            data = fetch_json(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            results = race.get("sprintQualyResults", [])
            if not results:
                continue

            for res in results:
                try:
                    cur.execute(
                        """
                        INSERT INTO f1_sprint_qualy_results
                        (season, round, race_id, driver_id, team_id, grid_position, sq1, sq2, sq3)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            season,
                            int(race["round"]),
                            race["raceId"],
                            res["driverId"],
                            res["teamId"],
                            res.get("gridPosition"),
                            res.get("sq1"),
                            res.get("sq2"),
                            res.get("sq3"),
                        ),
                    )
                    inserted += 1
                except Exception:
                    conn.rollback()
                    continue

            conn.commit()

    print(f"✅ f1_sprint_qualy_results: {inserted} rows")


# ---------------------------------------------------
# SPRINT RACE
# ---------------------------------------------------
def import_sprint_race():
    print("🏁 Importing sprint race")

    inserted = 0

    for season in SEASONS:
        for rnd in ROUNDS:
            url = f"{BASE_URL}/{season}/{rnd}/sprint/race?limit=30"
            data = fetch_json(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            results = race.get("sprintRaceResults", [])
            if not results:
                continue

            for res in results:
                try:
                    cur.execute(
                        """
                        INSERT INTO f1_sprint_race_results
                        (season, round, race_id, driver_id, team_id, position, grid_position, points)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            season,
                            int(race["round"]),
                            race["raceId"],
                            res["driverId"],
                            res["teamId"],
                            res.get("position"),
                            res.get("gridPosition"),
                            res.get("points"),
                        ),
                    )
                    inserted += 1
                except Exception:
                    conn.rollback()
                    continue

            conn.commit()

    print(f"✅ f1_sprint_race_results: {inserted} rows")


# ---------------------------------------------------
# RACE
# ---------------------------------------------------
def import_race():
    print("🏆 Importing race results")

    inserted = 0

    for season in SEASONS:
        for rnd in ROUNDS:
            url = f"{BASE_URL}/{season}/{rnd}/race?limit=30"
            data = fetch_json(url)
            if not data or "races" not in data:
                continue

            race = data["races"]
            results = race.get("results", [])
            if not results:
                continue

            for res in results:
                status = "Finished"
                if res.get("retired"):
                    status = "DNF"

                try:
                    cur.execute(
                        """
                        INSERT INTO f1_race_results
                        (season, round, race_id, driver_id, team_id, position, status)
                        VALUES (%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                        """,
                        (
                            season,
                            int(race["round"]),
                            race["raceId"],
                            res["driver"]["driverId"],
                            res["team"]["teamId"],
                            res.get("position"),
                            status,
                        ),
                    )
                    inserted += 1
                except Exception:
                    conn.rollback()
                    continue

            conn.commit()

    print(f"✅ f1_race_results: {inserted} rows")


# ---------------------------------------------------
# RUN ALL
# ---------------------------------------------------
import_fp("fp1", "f1_fp1_results", "fp1Results")
import_fp("fp2", "f1_fp2_results", "fp2Results")
import_fp("fp3", "f1_fp3_results", "fp3Results")

import_qualy()
import_sprint_qualy()
import_sprint_race()
import_race()

cur.close()
conn.close()

print("🎉 AUTO PIPELINE COMPLETE")
