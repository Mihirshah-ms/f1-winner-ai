import os
import requests
import psycopg2
from psycopg2.extras import execute_batch

DATABASE_URL = os.environ.get("DATABASE_URL")

PRIMARY = {
    "fp": "https://f1connectapi.vercel.app/api",
    "qualy": "https://f1api.dev/api",
    "race": "https://f1api.dev/api",
    "sprint": "https://f1api.dev/api"
}

FALLBACK = {
    "fp": "https://f1api.dev/api",
    "qualy": "https://f1connectapi.vercel.app/api",
    "race": "https://f1connectapi.vercel.app/api",
    "sprint": "https://f1connectapi.vercel.app/api"
}

SEASONS = [2024, 2025]
MAX_ROUNDS = 24

def get_conn():
    return psycopg2.connect(DATABASE_URL)

def fetch_json(url):
    r = requests.get(url, timeout=20)
    if r.status_code != 200:
        return None
    return r.json()

def try_api(primary_url, fallback_url):
    data = fetch_json(primary_url)
    if data and data.get("races"):
        print(f"✅ Primary API used → {primary_url}")
        return data, "primary"
    data = fetch_json(fallback_url)
    if data and data.get("races"):
        print(f"⚠️ Fallback API used → {fallback_url}")
        return data, "fallback"
    print(f"❌ No data from either API")
    return None, None

def import_fp(session):
    table = f"f1_{session}_results"
    print(f"🏎️ Importing {session.upper()}")

    rows = []
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            p = f"{PRIMARY['fp']}/{season}/{rnd}/{session}"
            f = f"{FALLBACK['fp']}/{season}/{rnd}/{session}"

            data, _ = try_api(p, f)
            if not data:
                continue

            r = data["races"]
            results = r.get(f"{session}Results", [])
            for res in results:
                rows.append((
                    season,
                    int(r["round"]),
                    r["raceId"],
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("time")
                ))

    if not rows:
        print(f"⚠️ {session}: no rows")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, f"""
                INSERT INTO {table}
                (season, round, race_id, driver_id, team_id, best_time)
                VALUES (%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, rows)
        conn.commit()

    print(f"✅ {table}: {len(rows)} rows")

def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            p = f"{PRIMARY['qualy']}/{season}/{rnd}/qualifying"
            f = f"{FALLBACK['qualy']}/{season}/{rnd}/qualy"

            data, _ = try_api(p, f)
            if not data:
                continue

            r = data["races"]
            results = r.get("qualifyingResults") or r.get("qualyResults") or []
            for res in results:
                rows.append((
                    season,
                    int(r["round"]),
                    r["raceId"],
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("q1"),
                    res.get("q2"),
                    res.get("q3"),
                    res.get("gridPosition")
                ))

    if not rows:
        print("⚠️ Qualy: no rows")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO f1_qualifying_results
                (season, round, race_id, driver_id, team_id, q1_time, q2_time, q3_time, grid_position)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, rows)
        conn.commit()

    print(f"✅ f1_qualifying_results: {len(rows)} rows")

def import_race(kind="race"):
    table = "f1_race_results" if kind == "race" else "f1_sprint_race_results"
    label = "🏆 Race" if kind == "race" else "⚡ Sprint Race"
    print(f"{label} importing")

    rows = []
    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            p = f"{PRIMARY['race']}/{season}/{rnd}/{kind}"
            f = f"{FALLBACK['race']}/{season}/{rnd}/{kind}"

            data, _ = try_api(p, f)
            if not data:
                continue

            r = data["races"]
            results = r.get("results") or r.get("sprintRaceResults") or []
            for res in results:
                rows.append((
                    season,
                    int(r["round"]),
                    r["raceId"],
                    res["driver"]["driverId"],
                    res["team"]["teamId"],
                    res.get("position"),
                    res.get("retired")
                ))

    if not rows:
        print(f"⚠️ {table}: no rows")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, f"""
                INSERT INTO {table}
                (season, round, race_id, driver_id, team_id, position, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, rows)
        conn.commit()

    print(f"✅ {table}: {len(rows)} rows")

def import_sprint_qualy():
    print("⚡ Importing sprint qualifying")
    rows = []

    for season in SEASONS:
        for rnd in range(1, MAX_ROUNDS + 1):
            p = f"{PRIMARY['sprint']}/{season}/{rnd}/sprint/qualifying"
            f = f"{FALLBACK['sprint']}/{season}/{rnd}/sprint/qualy"

            data, _ = try_api(p, f)
            if not data:
                continue

            r = data["races"]
            results = r.get("sprintQualifyingResults") or r.get("sprintQualyResults") or []
            for res in results:
                rows.append((
                    season,
                    int(r["round"]),
                    r["raceId"],
                    res.get("driverId"),
                    res.get("teamId"),
                    res.get("sq1"),
                    res.get("sq2"),
                    res.get("sq3"),
                    res.get("gridPosition")
                ))

    if not rows:
        print("⚠️ Sprint Qualy: no rows")
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, """
                INSERT INTO f1_sprint_qualy_results
                (season, round, race_id, driver_id, team_id, sq1_time, sq2_time, sq3_time, grid_position)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, rows)
        conn.commit()

    print(f"✅ f1_sprint_qualy_results: {len(rows)} rows")

# =========================
# 🚀 PIPELINE ENTRY POINT
# =========================
if __name__ == "__main__":
    print("🚀 AUTO PIPELINE STARTED")

    import_fp("fp1")
    import_fp("fp2")
    import_fp("fp3")

    import_qualy()
    import_sprint_qualy()
    import_race("sprint/race")
    import_race("race")

    print("🎉 AUTO PIPELINE COMPLETE")