import os
import psycopg2
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestRegressor

# =====================================================
# CONFIG
# =====================================================
DATABASE_URL = os.getenv("DATABASE_URL")
MODEL_PATH = "model.pkl"

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

print("🚀 TRAINING STARTED")

# =====================================================
# DB CONNECT
# =====================================================
conn = psycopg2.connect(DATABASE_URL)

# =====================================================
# LOAD SOURCE DATA (NO FEATURE TABLES)
# =====================================================
race = pd.read_sql("""
SELECT
    season,
    round,
    race_id,
    driver_id,
    team_id,
    position AS race_position,
    points   AS race_points
FROM f1_race_results
WHERE season < 2026
""", conn)

qualy = pd.read_sql("""
SELECT
    season,
    round,
    driver_id,
    grid_position,
    q1, q2, q3
FROM f1_qualifying_results
""", conn)

fp1 = pd.read_sql("""
SELECT season, round, driver_id, best_time AS fp1_time
FROM f1_fp1_results
""", conn)

fp2 = pd.read_sql("""
SELECT season, round, driver_id, best_time AS fp2_time
FROM f1_fp2_results
""", conn)

fp3 = pd.read_sql("""
SELECT season, round, driver_id, best_time AS fp3_time
FROM f1_fp3_results
""", conn)

sprint_q = pd.read_sql("""
SELECT season, round, driver_id, grid_position AS sprint_grid
FROM f1_sprint_qualy_results
""", conn)

sprint_r = pd.read_sql("""
SELECT season, round, driver_id, position AS sprint_finish
FROM f1_sprint_race_results
""", conn)

conn.close()

print(f"📊 Race rows loaded: {len(race)}")

# =====================================================
# FEATURE BUILDING (IN PYTHON)
# =====================================================
df = (
    race
    .merge(qualy, on=["season", "round", "driver_id"], how="left")
    .merge(fp1,   on=["season", "round", "driver_id"], how="left")
    .merge(fp2,   on=["season", "round", "driver_id"], how="left")
    .merge(fp3,   on=["season", "round", "driver_id"], how="left")
    .merge(sprint_q, on=["season", "round", "driver_id"], how="left")
    .merge(sprint_r, on=["season", "round", "driver_id"], how="left")
)

print("🧩 Feature table shape:", df.shape)

# =====================================================
# MODEL INPUTS
# =====================================================
FEATURES = [
    "grid_position",
    "fp1_time",
    "fp2_time",
    "fp3_time",
    "sprint_grid",
    "sprint_finish",
]

X = df[FEATURES]
y = df["race_position"]

# Ensure numeric only
X = X.apply(pd.to_numeric, errors="coerce")

# Fill missing numeric values safely
X = X.fillna(X.median())

# =====================================================
# TRAIN MODEL
# =====================================================
model = RandomForestRegressor(
    n_estimators=250,
    random_state=42,
    n_jobs=-1
)

model.fit(X, y)

joblib.dump(model, MODEL_PATH)

print(f"✅ MODEL TRAINED & SAVED → {MODEL_PATH}")