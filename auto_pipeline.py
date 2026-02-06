import os
import time
import requests
import psycopg2
from psycopg2.extras import execute_batch
from datetime import date, datetime

# ============================================================
# CONFIG (2026 ONLY)
# ============================================================
BASE_URL = "https://f1api.dev/api"
SEASON = 2026
MAX_ROUNDS = 24
SLEEP_SECONDS = 1.2

DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set")

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


def days_to_race(race_date):
    if not race_date:
        return None
    if isinstance(race_date, str):
        race_date = datetime.strptime(race_date, "%Y-%m-%d").date()
    return (race_date - date.today()).days


def exists(table, season, rnd):
    cur.execute(
        f"SELECT 1 FROM {table} WHERE season=%s AND round=%s LIMIT 1",
        (season, rnd),
    )
    return cur.fetchone() is not None


# ============================================================
# STATIC CIRCUIT COORDS (expand anytime)
# ============================================================
CIRCUIT_COORDS = {
    # 🇧🇭 Middle East
    "Bahrain International Circuit": (26.0325, 50.5106),
    "Jeddah Corniche Circuit": (21.6319, 39.1044),
    "Yas Marina Circuit": (24.4672, 54.6031),
    "Losail International Circuit": (25.4889, 51.4542),

    # 🇪🇺 Europe
    "Circuit de Monaco": (43.7347, 7.4206),
    "Circuit de Barcelona-Catalunya": (41.5700, 2.2611),
    "Circuit Paul Ricard": (43.2506, 5.7917),
    "Silverstone Circuit": (52.0786, -1.0169),
    "Autodromo Nazionale di Monza": (45.6156, 9.2811),
    "Red Bull Ring": (47.2197, 14.7647),
    "Hungaroring": (47.5789, 19.2486),
    "Circuit de Spa-Francorchamps": (50.4372, 5.9714),
    "Circuit Zandvoort": (52.3888, 4.5409),
    "Autodromo Enzo e Dino Ferrari": (44.3439, 11.7167),

    # 🇦🇺 / 🇯🇵 Asia-Pacific
    "Albert Park Circuit": (-37.8497, 144.9680),
    "Suzuka Circuit": (34.8431, 136.5419),
    "Marina Bay Street Circuit": (1.2914, 103.8644),
    "Shanghai International Circuit": (31.3389, 121.2196),

    # 🇺🇸 Americas
    "Circuit of the Americas": (30.1328, -97.6411),
    "Miami International Autodrome": (25.9581, -80.2389),
    "Autódromo Hermanos Rodríguez": (19.4042, -99.0907),
    "Autódromo José Carlos Pace": (-23.7036, -46.6997),
    "Gilles Villeneuve Circuit": (45.5006, -73.5228),

    # 🇦🇿 / 🇸🇦 Street Circuits
    "Baku City Circuit": (40.3725, 49.8533),
}

MECHANICAL_DNF_KEYWORDS = [
    # Power unit
    "engine",
    "power unit",
    "pu",
    "internal combustion",
    "ice",
    "turbo",
    "ers",
    "mgu-k",
    "mgu-h",
    "battery",

    # Transmission
    "gearbox",
    "clutch",
    "transmission",
    "driveshaft",

    # Hydraulics & electronics
    "hydraulics",
    "electrical",
    "electronics",
    "control electronics",
    "ecu",
    "software",

    # Brakes & steering
    "brake",
    "brakes",
    "brake failure",
    "steering",

    # Cooling & fluids
    "cooling",
    "overheating",
    "oil",
    "water leak",
    "fuel pressure",
    "fuel system",

    # Suspension / chassis
    "suspension",
    "chassis",
    "structural failure",

    # Generic F1 wording
    "mechanical",
    "technical problem",
    "car failure",
    "reliability",
]

# ============================================================
# RACE CALENDAR
# ============================================================
def import_race_calendar():
    print("📅 Importing race calendar")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_races", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data:
            continue

        race = data["race"][0]
        sched = race.get("schedule", {})
        circuit = race.get("circuit", {})

        rows.append((
            race.get("raceId"),
            SEASON,
            rnd,
            race.get("raceName"),
            sched.get("race", {}).get("date"),
            sched.get("race", {}).get("time"),
            sched.get("qualy", {}).get("date"),
            sched.get("qualy", {}).get("time"),
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
             race_date, race_time, qualy_date, qualy_time,
             circuit_name, circuit_country, laps)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season, round) DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_races: {len(rows)} rows")


# ============================================================
# FP SESSIONS
# ============================================================
def import_fp(session, table, key):
    print(f"🏎️ Importing {session.upper()}")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists(table, SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data:
            continue

        race = data["race"][0]
        for r in race.get(key, []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r.get("driverId"),
                r.get("teamId"),
                r.get("time"),
            ))

    if rows:
        execute_batch(
            cur,
            f"""
            INSERT INTO {table}
            (season, round, race_id, driver_id, team_id, best_time)
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ {table}: {len(rows)} rows")


# ============================================================
# QUALIFYING
# ============================================================
def import_qualy():
    print("⏱️ Importing qualifying")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_qualifying_results", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data:
            continue

        race = data["race"][0]
        for r in race.get("qualyResults", []):
            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r.get("driverId"),
                r.get("teamId"),
                r.get("q1"),
                r.get("q2"),
                r.get("q3"),
                safe_int(r.get("gridPosition")),
            ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, team_id,
             q1, q2, q3, grid_position)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_qualifying_results: {len(rows)} rows")


# ============================================================
# WEATHER (RACE-WEEK ONLY)
# ============================================================
def import_weather():
    print("🌦️ Importing weather (race-week only)")
    rows = []

    cur.execute("""
        SELECT season, round, race_id, race_date, circuit_name
        FROM f1_races
        WHERE season=%s
    """, (SEASON,))
    races = cur.fetchall()

    for season, rnd, race_id, race_date, circuit in races:
        delta = days_to_race(race_date)
        if delta is None or delta < 0 or delta > 7:
            continue
        if circuit not in CIRCUIT_COORDS:
            continue

        lat, lon = CIRCUIT_COORDS[circuit]
        url = (
            "https://api.open-meteo.com/v1/forecast"
            f"?latitude={lat}&longitude={lon}"
            "&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max"
            "&timezone=UTC"
        )

        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "daily" not in data:
            continue

        d = data["daily"]

        rows.append((
            season,
            rnd,
            race_id,
            d["time"][0],
            (d["temperature_2m_max"][0] + d["temperature_2m_min"][0]) / 2,
            d["temperature_2m_max"][0],
            d["temperature_2m_min"][0],
            d["precipitation_sum"][0],
            d["windspeed_10m_max"][0],
        ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_weather
            (season, round, race_id, weather_date,
             temp_avg, temp_max, temp_min, precipitation, wind_speed)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_weather: {len(rows)} rows")


def cleanup_weather():
    print("🧹 Cleaning completed-race weather")
    cur.execute("""
        DELETE FROM f1_weather
        WHERE (season, round) IN (
            SELECT r.season, r.round
            FROM f1_races r
            JOIN f1_race_results rr
              ON r.season = rr.season AND r.round = rr.round
        )
    """)
    conn.commit()


# ============================================================
# RACE RESULTS + MECHANICAL DNF
# ============================================================
def import_race_results():
    print("🏆 Importing race results")
    rows = []

    for rnd in range(1, MAX_ROUNDS + 1):
        if exists("f1_race_results", SEASON, rnd):
            continue

        url = f"{BASE_URL}/{SEASON}/{rnd}"
        data = fetch_json(url)
        time.sleep(SLEEP_SECONDS)

        if not data or "race" not in data:
            continue

        race = data["race"][0]

        for r in race.get("results", []):
            status = (r.get("retired") or "").lower()

            if any(k in status for k in MECHANICAL_DNF):
                cur.execute("""
                    INSERT INTO f1_dnf
                    (season, round, race_id, driver_id, team_id, dnf_reason)
                    VALUES (%s,%s,%s,%s,%s,%s)
                    ON CONFLICT DO NOTHING
                """, (
                    SEASON, rnd, race.get("raceId"),
                    r["driver"]["driverId"],
                    r["team"]["teamId"],
                    status
                ))

            rows.append((
                SEASON,
                rnd,
                race.get("raceId"),
                r["driver"]["driverId"],
                r["team"]["teamId"],
                safe_int(r.get("position")),
                safe_int(r.get("grid")),
                safe_int(r.get("points")),
                r.get("time"),
                status,
            ))

    if rows:
        execute_batch(
            cur,
            """
            INSERT INTO f1_race_results
            (season, round, race_id, driver_id, team_id,
             position, grid, points, race_time, status)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            rows,
        )
        conn.commit()

    print(f"✅ f1_race_results: {len(rows)} rows")


# ============================================================
# RUN ORDER
# ============================================================
import_race_calendar()
import_fp("fp1Results", "f1_fp1_results", "fp1Results")
import_fp("fp2Results", "f1_fp2_results", "fp2Results")
import_fp("fp3Results", "f1_fp3_results", "fp3Results")
import_qualy()
import_weather()
cleanup_weather()
import_race_results()

cur.close()
conn.close()
print("🎉 AUTO PIPELINE COMPLETE (2026)")