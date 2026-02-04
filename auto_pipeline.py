# =========================
# F1 AUTO PIPELINE (FINAL)
# =========================

import os
import psycopg2
import requests
import pandas as pd
import numpy as np
import pickle
import random
from collections import defaultdict
from datetime import datetime

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.metrics import accuracy_score

DATABASE_URL = os.getenv("DATABASE_URL")
API_BASE = "https://f1api.dev/api"
N_SIMULATIONS = 10000

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

# ---------------- UTILS ----------------
def safe_int(v):
    try:
        return int(v)
    except:
        return None

def fetch_json(url):
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    return r.json()

# ---------------- DB ----------------
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("🚀 AUTO PIPELINE STARTED")

# ---------------- SEASON DETECTION ----------------
cur.execute("SELECT COALESCE(MAX(season), 2024) FROM f1_races;")
season = cur.fetchone()[0]

# ---------------- INGEST RACES ----------------
data = fetch_json(f"{API_BASE}/{season}")
for race in data.get("races", []):
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

# ---------------- QUALIFYING ----------------
cur.execute("DELETE FROM f1_qualifying_results;")
cur.execute("SELECT season, round, race_id FROM f1_races;")

for season, rnd, race_id in cur.fetchall():
    try:
        q = fetch_json(f"{API_BASE}/{season}/{rnd}/qualy")
        for r in q.get("races", {}).get("qualyResults", []):
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

for season, rnd, race_id in cur.execute("SELECT season, round, race_id FROM f1_races;"):
    try:
        r = fetch_json(f"{API_BASE}/{season}/{rnd}/race")
        for res in r.get("races", {}).get("results", []):
            pos = safe_int(res.get("position"))
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
  q.qualy_score,
  d.avg_finish_5,
  c.avg_team_finish_24,
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

model_bytes = pickle.dumps(pipeline)

cur.execute("""
INSERT INTO ml_models (model_name, model_blob, trained_at, accuracy)
VALUES (%s, %s, %s, %s)
ON CONFLICT (model_name)
DO UPDATE SET
  model_blob = EXCLUDED.model_blob,
  trained_at = EXCLUDED.trained_at,
  accuracy = EXCLUDED.accuracy;
""", (
    "f1_winner_model",
    psycopg2.Binary(model_bytes),
    datetime.utcnow(),
    acc
))

cur.execute("""
INSERT INTO model_logs (run_time, accuracy)
VALUES (%s,%s);
""", (datetime.utcnow(), acc))

# ---------------- MONTE CARLO CHAMPIONSHIP ----------------
print("🏆 Running Championship Monte Carlo")

POINTS = {1:25,2:18,3:15,4:12,5:10,6:8,7:6,8:4,9:2,10:1}

points_df = pd.read_sql("""
SELECT driver_id, SUM(
  CASE position
    WHEN 1 THEN 25 WHEN 2 THEN 18 WHEN 3 THEN 15
    WHEN 4 THEN 12 WHEN 5 THEN 10 WHEN 6 THEN 8
    WHEN 7 THEN 6 WHEN 8 THEN 4 WHEN 9 THEN 2
    WHEN 10 THEN 1 ELSE 0 END
) AS points
FROM f1_race_results
GROUP BY driver_id;
""", conn)

current_points = dict(zip(points_df.driver_id, points_df.points))

pred_df = pd.read_sql("""
SELECT
  q.driver_id,
  q.qualy_score,
  d.avg_finish_5,
  c.avg_team_finish_24
FROM f1_qualifying_features q
LEFT JOIN f1_driver_recent_form d
  ON q.driver_id = d.driver_id AND q.round = d.round
LEFT JOIN f1_constructor_strength c
  ON q.round = c.round;
""", conn)

X = pred_df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
pred_df["p"] = pipeline.predict_proba(X)[:,1]

champ_count = defaultdict(int)
exp_pts = defaultdict(float)
random.seed(42)

for _ in range(N_SIMULATIONS):
    sim = current_points.copy()
    sample = pred_df.sample(
        n=min(10,len(pred_df)),
        weights=pred_df["p"],
        replace=False
    )
    for i,row in enumerate(sample.itertuples(),1):
        pts = POINTS.get(i,0)
        sim[row.driver_id] = sim.get(row.driver_id,0)+pts
        exp_pts[row.driver_id]+=pts/N_SIMULATIONS
    champ = max(sim,key=sim.get)
    champ_count[champ]+=1

cur.execute("DELETE FROM f1_championship_projection;")

for d in pred_df.driver_id.unique():
    cur.execute("""
        INSERT INTO f1_championship_projection
        (season, driver_id, current_points,
         expected_future_points, projected_total_points,
         championship_probability)
        VALUES (%s,%s,%s,%s,%s,%s);
    """, (
        season,
        d,
        current_points.get(d,0),
        round(exp_pts.get(d,0),2),
        round(current_points.get(d,0)+exp_pts.get(d,0),2),
        round(champ_count.get(d,0)/N_SIMULATIONS,4)
    ))

conn.commit()
conn.close()

print(f"✅ PIPELINE COMPLETE | Accuracy: {acc:.3f}")
