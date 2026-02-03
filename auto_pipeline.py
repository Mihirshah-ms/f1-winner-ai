# auto_pipeline.py
import os
import psycopg2
import requests
import datetime
import pandas as pd
import numpy as np
import pickle

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

DATABASE_URL = os.getenv("DATABASE_URL")
API_BASE = "https://f1api.dev/api"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# ---------------- UTILS ----------------
def safe_int(val):
    try:
        return int(val)
    except:
        return None

def safe_float(val):
    try:
        return float(val)
    except:
        return None

def fetch_json(url):
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

# ---------------- DB ----------------
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("🚀 F1 AUTO PIPELINE STARTED")

# ---------------- SEASON DETECTION ----------------
cur.execute("SELECT COALESCE(MAX(season), 2024) FROM f1_races;")
current_season = cur.fetchone()[0]

seasons = [current_season, current_season + 1]

# ---------------- INGEST RACES ----------------
for season in seasons:
    try:
        data = fetch_json(f"{API_BASE}/{season}")
        races = data.get("races", [])

        for race in races:
            circuit = race.get("circuit", {})
            cur.execute("""
            INSERT INTO f1_races (
                race_id, season, round, race_name,
                race_date, race_time,
                circuit_name, circuit_country, laps
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (race_id) DO NOTHING;
            """, (
                race.get("raceId"),
                season,
                safe_int(race.get("round")),
                race.get("raceName"),
                race.get("date"),
                race.get("time"),
                circuit.get("circuitName"),
                circuit.get("country"),
                safe_int(race.get("laps"))
            ))

        conn.commit()
    except Exception as e:
        print(f"⚠️ Race ingest failed for {season}: {e}")

# ---------------- QUALIFYING ----------------
cur.execute("DELETE FROM f1_qualifying_results;")

cur.execute("SELECT season, round, race_id FROM f1_races;")
races = cur.fetchall()

for season, rnd, race_id in races:
    try:
        q = fetch_json(f"{API_BASE}/{season}/{rnd}/qualy")
        results = q.get("races", {}).get("qualyResults", [])

        for r in results:
            cur.execute("""
            INSERT INTO f1_qualifying_results
            (season, round, race_id, driver_id, position, q1_time, q2_time, q3_time)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s);
            """, (
                season, rnd, race_id,
                r.get("driverId"),
                safe_int(r.get("gridPosition")),
                r.get("q1"), r.get("q2"), r.get("q3")
            ))

        conn.commit()
    except:
        continue

# ---------------- RACE RESULTS ----------------
cur.execute("DELETE FROM f1_race_results;")

for season, rnd, race_id in races:
    try:
        r = fetch_json(f"{API_BASE}/{season}/{rnd}/race")
        results = r.get("races", {}).get("results", [])

        for res in results:
            pos = res.get("position")
            pos = safe_int(pos) if pos not in ["NC", "R", "-", None] else None

            cur.execute("""
            INSERT INTO f1_race_results
            (season, round, race_id, driver_id, team_id, position)
            VALUES (%s,%s,%s,%s,%s,%s);
            """, (
                season, rnd, race_id,
                res.get("driverId"),
                res.get("teamId"),
                pos
            ))

        conn.commit()
    except:
        continue

# ---------------- FEATURES ----------------
cur.execute("DROP TABLE IF EXISTS f1_driver_recent_form;")
cur.execute("""
CREATE TABLE f1_driver_recent_form AS
SELECT
    r1.season, r1.round, r1.driver_id,
    AVG(r2.position) AS avg_finish_5
FROM f1_race_results r1
JOIN f1_race_results r2
ON r1.driver_id = r2.driver_id
AND r2.round < r1.round
AND r2.round >= r1.round - 5
GROUP BY r1.season, r1.round, r1.driver_id;
""")

cur.execute("DROP TABLE IF EXISTS f1_constructor_strength;")
cur.execute("""
CREATE TABLE f1_constructor_strength AS
SELECT
    r1.season, r1.round, r1.team_id,
    AVG(r2.position) AS avg_team_finish_24
FROM f1_race_results r1
JOIN f1_race_results r2
ON r1.team_id = r2.team_id
AND r2.round < r1.round
AND r2.round >= r1.round - 24
GROUP BY r1.season, r1.round, r1.team_id;
""")

cur.execute("DROP TABLE IF EXISTS f1_training_data;")
cur.execute("""
CREATE TABLE f1_training_data AS
SELECT
    q.season, q.round, q.driver_id,
    q.qualy_score,
    d.avg_finish_5,
    c.avg_team_finish_24,
    r.position,
    CASE WHEN r.position = 1 THEN 1 ELSE 0 END AS winner
FROM f1_qualifying_features q
JOIN f1_race_results r
ON q.season = r.season AND q.round = r.round AND q.driver_id = r.driver_id
LEFT JOIN f1_driver_recent_form d
ON q.driver_id = d.driver_id AND q.round = d.round
LEFT JOIN f1_constructor_strength c
ON r.team_id = c.team_id AND q.round = c.round;
""")

conn.commit()

# ---------------- MODEL TRAINING ----------------
df = pd.read_sql("""
SELECT qualy_score, avg_finish_5, avg_team_finish_24, winner
FROM f1_training_data
WHERE winner IS NOT NULL;
""", conn)

X = df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
y = df["winner"]

pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("model", HistGradientBoostingClassifier(max_depth=6))
])

pipeline.fit(X, y)
acc = accuracy_score(y, pipeline.predict(X))

with open("model.pkl", "wb") as f:
    pickle.dump(pipeline, f)

cur.execute("""
INSERT INTO model_logs (run_time, accuracy)
VALUES (%s,%s);
""", (datetime.datetime.utcnow(), acc))

conn.commit()
conn.close()

print(f"✅ PIPELINE COMPLETE | Accuracy: {acc:.3f}")