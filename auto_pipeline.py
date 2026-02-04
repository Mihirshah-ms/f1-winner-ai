import os
import requests
import psycopg2
import pandas as pd
import pickle
from datetime import datetime

print("🚀 AUTO PIPELINE STARTED")

# =========================
# DATABASE CONNECTION
# =========================
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("❌ DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# =========================
# HELPERS
# =========================
def safe_int(val):
    try:
        return int(val)
    except:
        return None

def fetch_json(url):
    try:
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return None
        return r.json()
    except:
        return None

# =========================
# STEP 1: IMPORT RACE RESULTS
# =========================
print("🏁 Importing race results")
inserted_races = 0

for season in [2024, 2025]:
    print(f"📥 Fetching race results for {season}")
    data = fetch_json(f"https://f1api.dev/api/{season}")

    if not data or "races" not in data:
        print(f"❌ Failed to fetch results for {season}")
        continue

    for race in data["races"]:
        race_id = race.get("raceId")
        rnd = safe_int(race.get("round"))

        results = race.get("results", [])
        if not results:
            continue

        for res in results:
            position = safe_int(res.get("position"))
            driver_id = res.get("driverId")
            team_id = res.get("teamId")
            status = res.get("status")

            cur.execute("""
                INSERT INTO f1_race_results
                (season, round, race_id, driver_id, team_id, position, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                season, rnd, race_id,
                driver_id, team_id,
                position, status
            ))
            inserted_races += 1

conn.commit()
print(f"✅ Race results import complete ({inserted_races} rows)")

# =========================
# STEP 2: IMPORT QUALIFYING
# =========================
print("🏎️ Importing qualifying results")
inserted_qualy = 0

for season in [2024, 2025]:
    print(f"📥 Fetching qualifying for {season}")
    data = fetch_json(f"https://f1api.dev/api/{season}")

    if not data or "races" not in data:
        print(f"❌ Failed to fetch qualifying for {season}")
        continue

    for race in data["races"]:
        race_id = race.get("raceId")
        rnd = safe_int(race.get("round"))
        qualy = race.get("qualifyingResults", [])

        for q in qualy:
            cur.execute("""
                INSERT INTO f1_qualifying_results
                (season, round, race_id, driver_id, team_id, position, q1_time, q2_time, q3_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                season, rnd, race_id,
                q.get("driverId"),
                q.get("teamId"),
                safe_int(q.get("position")),
                q.get("q1"),
                q.get("q2"),
                q.get("q3")
            ))
            inserted_qualy += 1

conn.commit()
print(f"✅ Qualifying import complete ({inserted_qualy} rows)")

# =========================
# STEP 3: LOAD TRAINING DATA
# =========================
print("📥 Loading training data")

df = pd.read_sql("""
SELECT
  q.driver_id,
  q.qualy_score,
  d.avg_finish_5 AS driver_form,
  c.avg_team_finish_24 AS team_strength,
  CASE WHEN r.position = 1 THEN 1 ELSE 0 END AS win
FROM f1_qualifying_features q
JOIN f1_race_results r
  ON q.season = r.season AND q.round = r.round AND q.driver_id = r.driver_id
LEFT JOIN f1_driver_recent_form d
  ON q.season = d.season AND q.round = d.round AND q.driver_id = d.driver_id
LEFT JOIN f1_constructor_strength c
  ON q.season = c.season AND q.round = c.round AND r.team_id = c.team_id
WHERE r.position IS NOT NULL
""", conn)

if df.empty:
    print("⚠️ No training data available yet. Skipping training.")
    conn.close()
    exit(0)

# =========================
# STEP 4: TRAIN MODEL
# =========================
print("🤖 Training model")

from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.ensemble import GradientBoostingClassifier

X = df[["qualy_score", "driver_form", "team_strength"]]
y = df["win"]

pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("model", GradientBoostingClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=4,
        random_state=42
    ))
])

pipeline.fit(X, y)

with open("model.pkl", "wb") as f:
    pickle.dump(pipeline, f)

print("✅ Model trained and saved (model.pkl)")
conn.close()
print("🎯 AUTO PIPELINE COMPLETE")