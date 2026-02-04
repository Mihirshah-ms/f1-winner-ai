import os
import psycopg2
import pandas as pd
import pickle
import requests
from datetime import datetime

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LogisticRegression

# ---------------- CONFIG ----------------
MODEL_NAME = "f1_winner_model"
MIN_ROWS_REQUIRED = 50
API_BASE = "https://f1connectapi.vercel.app/api"

print("🚀 AUTO PIPELINE STARTED")

# ---------------- DB CONNECTION ----------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# ---------------- HELPERS ----------------
def safe_position(pos):
    if pos in [None, "-", "NC", "DQ", "DNS", "DNF"]:
        return None
    try:
        return int(pos)
    except:
        return None

# ---------------- ENSURE TABLE ----------------
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_race_results (
    season INT,
    round INT,
    race_id TEXT,
    driver_id TEXT,
    team_id TEXT,
    position INT,
    PRIMARY KEY (season, round, driver_id)
);
""")
conn.commit()

# ---------------- IMPORT RACE RESULTS ----------------
print("🏁 Importing race results")

for season in [2024, 2025]:
    print(f"📥 Fetching race results for {season}")
    try:
        resp = requests.get(f"{API_BASE}/{season}", timeout=20)
        data = resp.json()
    except Exception as e:
        print(f"⚠️ Failed to fetch {season}: {e}")
        continue

    races = data.get("races", [])
    for race in races:
        round_no = race.get("round")
        race_id = race.get("raceId")
        results_block = race.get("results", {})
        results = results_block.get("raceResults", [])

        if not results:
            continue

        for res in results:
            position = safe_position(res.get("position"))

            cur.execute("""
            INSERT INTO f1_race_results (
                season, round, race_id, driver_id, team_id, position
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season, round, driver_id) DO NOTHING;
            """, (
                season,
                round_no,
                race_id,
                res.get("driverId"),
                res.get("teamId"),
                position
            ))

conn.commit()
print("✅ Race results import complete")

# ---------------- LOAD TRAINING DATA ----------------
print("📥 Loading training data")

df = pd.read_sql("""
SELECT
  q.driver_id,
  q.qualy_score,
  d.avg_finish_5        AS avg_driver_form,
  c.avg_team_finish_24  AS avg_team_strength,
  CASE WHEN r.position = 1 THEN 1 ELSE 0 END AS win
FROM f1_qualifying_features q
LEFT JOIN f1_race_results r
  ON q.season = r.season
 AND q.round = r.round
 AND q.driver_id = r.driver_id
LEFT JOIN f1_driver_recent_form d
  ON q.season = d.season
 AND q.round = d.round
 AND q.driver_id = d.driver_id
LEFT JOIN f1_constructor_strength c
  ON r.season = c.season
 AND r.round = c.round
 AND r.team_id = c.team_id
WHERE r.position IS NOT NULL;
""", conn)

# ---------------- SAFETY GUARDS ----------------
if df.empty:
    print("⚠️ No training data available yet. Skipping training.")
    conn.close()
    exit(0)

if len(df) < MIN_ROWS_REQUIRED:
    print(f"⚠️ Only {len(df)} rows available. Waiting for more races.")
    conn.close()
    exit(0)

print(f"✅ Training rows available: {len(df)}")

# ---------------- FEATURES / TARGET ----------------
X = df[
    ["qualy_score", "avg_driver_form", "avg_team_strength"]
]
y = df["win"]

# ---------------- ML PIPELINE ----------------
pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler()),
    ("model", LogisticRegression(
        max_iter=300,
        class_weight="balanced",
        n_jobs=1
    ))
])

# ---------------- TRAIN MODEL ----------------
print("🧠 Training model")
pipeline.fit(X, y)

accuracy = pipeline.score(X, y)
trained_at = datetime.utcnow()

print(f"🎯 Training complete | Accuracy: {accuracy:.3f}")

# ---------------- STORE MODEL ----------------
model_blob = pickle.dumps(pipeline)

cur.execute("""
DELETE FROM ml_models
WHERE model_name = %s;
""", (MODEL_NAME,))

cur.execute("""
INSERT INTO ml_models (model_name, model_blob, trained_at, accuracy)
VALUES (%s,%s,%s,%s);
""", (
    MODEL_NAME,
    psycopg2.Binary(model_blob),
    trained_at,
    accuracy
))

conn.commit()
conn.close()

print("💾 Model stored successfully")
print("✅ AUTO PIPELINE COMPLETE")
