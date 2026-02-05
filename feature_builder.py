
import os
import psycopg2
import pandas as pd

# ====================================
# DB CONNECTION
# ====================================
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DATABASE_URL)

# ====================================
# LOAD DATA
# ====================================
QUERY = """
SELECT
    r.season,
    r.round,
    r.race_id,
    rr.driver_id,
    rr.team_id,

    q.grid_position,
    q.q1, q.q2, q.q3,

    fp1.best_time AS fp1_time,
    fp2.best_time AS fp2_time,
    fp3.best_time AS fp3_time,

    sq.grid_position AS sprint_grid,
    sr.position      AS sprint_finish,

    rr.position AS race_position,
    rr.points   AS race_points

FROM f1_race_results rr
JOIN f1_races r
  ON r.season = rr.season
 AND r.round  = rr.round

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
"""

df = pd.read_sql(QUERY, conn)
conn.close()

# ====================================
# TIME CONVERSION
# ====================================
def time_to_seconds(t):
    if pd.isna(t):
        return None
    try:
        if ":" in str(t):
            m, s = str(t).split(":")
            return int(m) * 60 + float(s)
        return float(t)
    except:
        return None


TIME_COLS = ["q1", "q2", "q3", "fp1_time", "fp2_time", "fp3_time"]

for col in TIME_COLS:
    df[col] = df[col].apply(time_to_seconds)

# ====================================
# MISSING DATA HANDLING
# ====================================
df["grid_position"] = df["grid_position"].fillna(20)
df["sprint_grid"] = df["sprint_grid"].fillna(df["grid_position"])
df["sprint_finish"] = df["sprint_finish"].fillna(20)

df["fp3_time"] = df["fp3_time"].fillna(df["fp2_time"])
df["fp2_time"] = df["fp2_time"].fillna(df["fp1_time"])

for col in ["fp1_time", "fp2_time", "fp3_time"]:
    df[col] = df[col].fillna(df[col].median())

# ====================================
# ENCODE CATEGORIES
# ====================================
df["driver_code"] = df["driver_id"].astype("category").cat.codes
df["team_code"]   = df["team_id"].astype("category").cat.codes

# ====================================
# SPLIT DATA
# ====================================
FEATURES = [
    "grid_position",
    "q1", "q2", "q3",
    "fp1_time", "fp2_time", "fp3_time",
    "sprint_grid", "sprint_finish",
    "driver_code", "team_code",
    "round"
]

train_df = df[
    (df["season"] <= 2025) &
    (df["race_position"].notna())
]

predict_df = df[
    (df["season"] == 2026) &
    (df["race_position"].isna())
]

X_train = train_df[FEATURES]
y_train = train_df["race_position"]

X_pred = predict_df[FEATURES]

# ====================================
# EXPORT (OPTIONAL, SAFE)
# ====================================
os.makedirs("data", exist_ok=True)

X_train.to_csv("data/X_train.csv", index=False)
y_train.to_csv("data/y_train.csv", index=False)
X_pred.to_csv("data/X_pred_2026.csv", index=False)

print("✅ Feature engineering complete")
print(f"Training rows: {len(X_train)}")
print(f"Prediction rows (2026): {len(X_pred)}")