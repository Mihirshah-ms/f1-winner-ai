import os
import pickle
import psycopg2
import pandas as pd
from sklearn.preprocessing import StandardScaler

print("🔮 PREDICTION PIPELINE STARTED (2026)")

DB_URL = os.getenv("DATABASE_URL")
MODEL_PATH = "model.pkl"
SEASON = 2026

# ------------------------
# Load model
# ------------------------
if not os.path.exists(MODEL_PATH):
    raise FileNotFoundError("❌ model.pkl not found")

with open(MODEL_PATH, "rb") as f:
    model = pickle.load(f)

print("✅ Model loaded")

# ------------------------
# DB connect
# ------------------------
conn = psycopg2.connect(DB_URL)

# ------------------------
# Load feature data (NO race result)
# ------------------------
query = """
SELECT
    r.season,
    r.round,
    r.race_id,
    q.driver_id,
    q.team_id,

    q.grid_position,
    q.q1,
    q.q2,
    q.q3,

    fp1.best_time AS fp1_time,
    fp2.best_time AS fp2_time,
    fp3.best_time AS fp3_time,

    sq.grid_position AS sprint_grid,
    sr.position      AS sprint_finish

FROM f1_races r

LEFT JOIN f1_qualifying_results q
  ON r.season = q.season AND r.round = q.round

LEFT JOIN f1_fp1_results fp1
  ON fp1.season = q.season AND fp1.round = q.round AND fp1.driver_id = q.driver_id

LEFT JOIN f1_fp2_results fp2
  ON fp2.season = q.season AND fp2.round = q.round AND fp2.driver_id = q.driver_id

LEFT JOIN f1_fp3_results fp3
  ON fp3.season = q.season AND fp3.round = q.round AND fp3.driver_id = q.driver_id

LEFT JOIN f1_sprint_qualy_results sq
  ON sq.season = q.season AND sq.round = q.round AND sq.driver_id = q.driver_id

LEFT JOIN f1_sprint_race_results sr
  ON sr.season = q.season AND sr.round = q.round AND sr.driver_id = q.driver_id

WHERE r.season = 2026;
"""

df = pd.read_sql(query, conn)
print(f"📊 Rows loaded for prediction: {len(df)}")

if df.empty:
    print("⚠️ No data available yet for predictions")
    exit()

# ------------------------
# Prepare features
# ------------------------
meta_cols = ["season", "round", "race_id", "driver_id", "team_id"]
X = df.drop(columns=meta_cols)

# Convert times to numeric safely
for col in X.columns:
    X[col] = pd.to_numeric(X[col], errors="coerce")

X = X.fillna(X.median(numeric_only=True))

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# ------------------------
# Predict
# ------------------------
pred_positions = model.predict(X_scaled)

df["predicted_position"] = pred_positions
df["predicted_points"] = (21 - df["predicted_position"]).clip(lower=0)

# ------------------------
# Save to DB
# ------------------------
cur = conn.cursor()

rows = [
    (
        row.season,
        row.round,
        row.race_id,
        row.driver_id,
        row.team_id,
        int(row.predicted_position),
        float(row.predicted_points),
    )
    for row in df.itertuples()
]

cur.executemany(
    """
    INSERT INTO f1_predictions
    (season, round, race_id, driver_id, team_id, predicted_position, predicted_points)
    VALUES (%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (season, round, driver_id)
    DO UPDATE SET
        predicted_position = EXCLUDED.predicted_position,
        predicted_points   = EXCLUDED.predicted_points,
        created_at = NOW()
    """,
    rows,
)

conn.commit()
cur.close()
conn.close()

print("🎉 PREDICTIONS SAVED FOR 2026")