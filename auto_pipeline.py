import os
import psycopg2
import requests
import pandas as pd
import pickle
from datetime import datetime
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import StandardScaler
from sklearn.ensemble import GradientBoostingClassifier

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

# =========================
# STEP 1: IMPORT RACE RESULTS
# =========================
print("🏁 Importing race results")

def import_race_results(season):
    print(f"📥 Fetching race results for {season}")
    url = f"https://f1connectapi.vercel.app/api/{season}/results"
    res = requests.get(url, timeout=30)

    if res.status_code != 200:
        print(f"❌ Failed to fetch results for {season}")
        return 0

    races = res.json().get("races", [])
    inserted = 0

    for race in races:
        rnd = safe_int(race.get("round"))
        race_id = race.get("raceId")

        for r in race.get("results", []):
            position = safe_int(r.get("position"))
            status = r.get("status")

            cur.execute("""
                INSERT INTO f1_race_results (
                    season, round, race_id,
                    driver_id, team_id,
                    position, status
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT DO NOTHING
            """, (
                season,
                rnd,
                race_id,
                r.get("driverId"),
                r.get("teamId"),
                position,
                status
            ))
            inserted += 1

    conn.commit()
    return inserted

rows_2024 = import_race_results(2024)
rows_2025 = import_race_results(2025)

print(f"✅ Race results import complete ({rows_2024 + rows_2025} rows)")

print("🏎️ Importing qualifying results")

import requests

def safe_time(t):
    if t in [None, "-", "", "N/A"]:
        return None
    return t

seasons = [2024, 2025]

for season in seasons:
    print(f"📥 Fetching qualifying for {season}")

    races = cur.execute(
        "SELECT round, race_id FROM f1_races WHERE season = %s;",
        (season,)
    ).fetchall()

    for rnd, race_id in races:
        url = f"https://f1connectapi.vercel.app/api/{season}/{rnd}/qualy"
        r = requests.get(url, timeout=10)

        if r.status_code != 200:
            continue

        data = r.json()
        race = data.get("races")

        if not race or "qualyResults" not in race:
            continue

        for res in race["qualyResults"]:
            driver_id = res.get("driverId")
            team_id = res.get("teamId")
            pos_raw = res.get("gridPosition")

            position = (
                None if pos_raw in [None, "-", "NC", "DQ", "R"]
                else int(pos_raw)
            )

            q1 = safe_time(res.get("q1"))
            q2 = safe_time(res.get("q2"))
            q3 = safe_time(res.get("q3"))

            cur.execute("""
            INSERT INTO f1_qualifying_results (
                season, round, race_id,
                driver_id, team_id,
                position, q1_time, q2_time, q3_time
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (season, round, driver_id)
            DO UPDATE SET
                position = EXCLUDED.position,
                q1_time = EXCLUDED.q1_time,
                q2_time = EXCLUDED.q2_time,
                q3_time = EXCLUDED.q3_time;
            """, (
                season, rnd, race_id,
                driver_id, team_id,
                position, q1, q2, q3
            ))

conn.commit()
print("✅ Qualifying import complete")

# =========================
# STEP 2: LOAD TRAINING DATA
# =========================
print("📥 Loading training data")

df = pd.read_sql("""
SELECT
  q.driver_id,
  q.qualy_score,
  d.avg_finish_5,
  c.avg_team_finish_24,
  CASE WHEN r.position = 1 THEN 1 ELSE 0 END AS win
FROM f1_qualifying_features q
JOIN f1_race_results r
  ON q.season = r.season
 AND q.round = r.round
 AND q.driver_id = r.driver_id
LEFT JOIN f1_driver_recent_form d
  ON q.season = d.season
 AND q.round = d.round
 AND q.driver_id = d.driver_id
LEFT JOIN f1_constructor_strength c
  ON q.season = c.season
 AND q.round = c.round
 AND r.team_id = c.team_id
WHERE r.position IS NOT NULL
""", conn)

if df.empty or len(df) < 20:
    print("⚠️ No training data available yet. Skipping training.")
    conn.close()
    exit(0)

# =========================
# STEP 3: TRAIN MODEL
# =========================
print(f"🤖 Training model on {len(df)} rows")

X = df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
y = df["win"]

pipeline = Pipeline([
    ("imputer", SimpleImputer(strategy="median")),
    ("scaler", StandardScaler()),
    ("model", GradientBoostingClassifier(
        n_estimators=300,
        learning_rate=0.05,
        max_depth=3,
        random_state=42
    ))
])

pipeline.fit(X, y)

# =========================
# STEP 4: SAVE MODEL
# =========================
with open("model.pkl", "wb") as f:
    pickle.dump(pipeline, f)

print("✅ Model trained and saved as model.pkl")

conn.close()
print("🎯 AUTO PIPELINE COMPLETED SUCCESSFULLY")
