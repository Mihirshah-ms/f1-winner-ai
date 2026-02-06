import os
import psycopg2
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import joblib

print("🚀 TRAINING STARTED")

# -----------------------------
# DB CONNECTION
# -----------------------------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DB_URL)

# -----------------------------
# LOAD TRAINING DATA (2024–2025)
# -----------------------------
query = """
SELECT
    rr.season,
    rr.round,
    rr.driver_id,
    rr.team_id,

    q.grid_position,

    fp1.best_time AS fp1_time,
    fp2.best_time AS fp2_time,
    fp3.best_time AS fp3_time,

    sq.grid_position AS sprint_grid,
    sr.position      AS sprint_finish,

    rr.position AS race_position
FROM f1_race_results rr

LEFT JOIN f1_qualifying_results q
  ON q.season = rr.season
 AND q.round  = rr.round
 AND q.driver_id = rr.driver_id

LEFT JOIN f1_fp1_results fp1
  ON fp1.season = rr.season
 AND fp1.round  = rr.round
 AND fp1.driver_id = rr.driver_id

LEFT JOIN f1_fp2_results fp2
  ON fp2.season = rr.season
 AND fp2.round  = rr.round
 AND fp2.driver_id = rr.driver_id

LEFT JOIN f1_fp3_results fp3
  ON fp3.season = rr.season
 AND fp3.round  = rr.round
 AND fp3.driver_id = rr.driver_id

LEFT JOIN f1_sprint_qualy_results sq
  ON sq.season = rr.season
 AND sq.round  = rr.round
 AND sq.driver_id = rr.driver_id

LEFT JOIN f1_sprint_race_results sr
  ON sr.season = rr.season
 AND sr.round  = rr.round
 AND sr.driver_id = rr.driver_id

WHERE rr.season IN (2024, 2025)
"""

df = pd.read_sql(query, conn)
conn.close()

print(f"📊 Rows loaded: {len(df)}")

# -----------------------------
# CLEAN + PREP
# -----------------------------
df = df.dropna(subset=["race_position", "grid_position"])

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

X = X.fillna(X.median())

# -----------------------------
# TRAIN / TEST SPLIT
# -----------------------------
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# -----------------------------
# MODEL
# -----------------------------
model = RandomForestRegressor(
    n_estimators=200,
    random_state=42,
    n_jobs=-1
)

model.fit(X_train, y_train)

# -----------------------------
# EVALUATION
# -----------------------------
preds = model.predict(X_test)
mae = mean_absolute_error(y_test, preds)

print(f"📉 MAE: {mae:.2f} positions")

# -----------------------------
# SAVE MODEL
# -----------------------------
joblib.dump(model, "model.pkl")
print("💾 model.pkl saved")
print("🎉 TRAINING COMPLETE")
