import os
import requests
import psycopg2
from psycopg2.extras import execute_batch

print("🚀 AUTO PIPELINE STARTED")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()


# ------------------------
# UTILITIES
# ------------------------

def fetch_json(url):
    try:
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None


def get_valid_rounds(season):
    url = f"https://f1api.dev/api/{season}"
    data = fetch_json(url)
    if not data or "races" not in data:
        return []
    return [int(r["round"]) for r in data["races"]]


def log_api(label, api_name, season, rnd):
    print(f"🔗 {label} | {api_name} | {season} R{rnd}")


# ------------------------
# FP IMPORT (Primary: f1connectapi, Fallback: f1api.dev)
# ------------------------

def import_fp(session, table):
    print(f"🏎️ Importing {session.upper()}")
    rows = []

    for season in [2024, 2025]:
        rounds = get_valid_rounds(season)
        for rnd in rounds:

            # PRIMARY
            url = f"https://f1connectapi.vercel.app/api/{season}/{rnd}/{session}"
            data = fetch_json(url)

            if data and "races" in data and f"{session}Results" in data["races"]:
                log_api(session, "f1connectapi", season, rnd)
                results = data["races"][f"{session}Results"]
            else:
                # FALLBACK
                url = f"https://f1api.dev/api/{season}/{rnd}/{session}"
                data = fetch_json(url)
                if not data or "races" not in data or "results" not in data["races"]:
                    continue
                log_api(session, "f1api.dev", season, rnd)
                results = data["races"]["results"]

            for r in results:
                rows.append((
                    season,
                    rnd,
                    r["driverId"],
                    r["teamId"],
                    r.get("time")
                ))

    if not rows:
        print(f"⚠️ No data found for {session.upper()} (this is normal for some weekends)")
        return

    execute_batch(cur, f"""
        INSERT INTO {table} (season, round, driver_id, team_id, best_time)
        VALUES (%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ {table}: {len(rows)} rows")


# ------------------------
# QUALIFYING (Primary: f1api.dev)
# ------------------------

def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for season in [2024, 2025]:
        for rnd in get_valid_rounds(season):

            url = f"https://f1api.dev/api/{season}/{rnd}/qualy"
            data = fetch_json(url)

            if data and "races" in data and "qualifyingResults" in data["races"]:
                log_api("qualy", "f1api.dev", season, rnd)
                results = data["races"]["qualifyingResults"]
            else:
                url = f"https://f1connectapi.vercel.app/api/{season}/{rnd}/qualy"
                data = fetch_json(url)
                if not data or "races" not in data or "qualyResults" not in data["races"]:
                    continue
                log_api("qualy", "f1connectapi", season, rnd)
                results = data["races"]["qualyResults"]

            for r in results:
                rows.append((
                    season,
                    rnd,
                    r["driverId"],
                    r["teamId"],
                    r.get("q1"),
                    r.get("q2"),
                    r.get("q3"),
                    r.get("gridPosition")
                ))

    if not rows:
        print("⚠️ No qualifying data available yet — skipping")
        return

    execute_batch(cur, """
        INSERT INTO f1_qualifying_results
        (season, round, driver_id, team_id, q1_time, q2_time, q3_time, grid_position)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ f1_qualifying_results: {len(rows)} rows")


# ------------------------
# RACE RESULTS (Primary: f1api.dev)
# ------------------------

def import_race():
    print("🏆 Importing race results")
    rows = []

    for season in [2024, 2025]:
        for rnd in get_valid_rounds(season):

            url = f"https://f1api.dev/api/{season}/{rnd}/race"
            data = fetch_json(url)

            if data and "races" in data and "results" in data["races"]:
                log_api("race", "f1api.dev", season, rnd)
                race = data["races"]
                results = race["results"]
            else:
                url = f"https://f1connectapi.vercel.app/api/{season}/{rnd}/race"
                data = fetch_json(url)
                if not data or "races" not in data or "results" not in data["races"]:
                    continue
                log_api("race", "f1connectapi", season, rnd)
                race = data["races"]
                results = race["results"]

            for r in results:
                rows.append((
                    season,
                    rnd,
                    race["raceId"],
                    r["driver"]["driverId"],
                    r["team"]["teamId"],
                    r.get("position"),
                    "Finished"
                ))

    if not rows:
        raise RuntimeError("❌ No race rows parsed")

    execute_batch(cur, """
        INSERT INTO f1_race_results
        (season, round, race_id, driver_id, team_id, position, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT DO NOTHING
    """, rows)

    conn.commit()
    print(f"✅ f1_race_results: {len(rows)} rows")


# ------------------------
# RUN PIPELINE
# ------------------------

import_fp("fp1", "f1_fp1_results")
import_fp("fp2", "f1_fp2_results")
import_fp("fp3", "f1_fp3_results")
import_qualy()
import_race()

print("🎉 AUTO PIPELINE COMPLETE")
