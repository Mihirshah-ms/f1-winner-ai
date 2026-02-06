import os
import psycopg2
import pandas as pd
import joblib
from sklearn.ensemble import RandomForestClassifier

print("🚀 Training started")

# -------------------------
# DB Connection
# -------------------------
DB_URL = os.getenv("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set")

conn = psycopg2.connect(DB_URL)

# -------------------------
# Load training data
# -------------------------
query = """
SELECT *
FROM f1_ml_features
WHERE race_position IS NOT NULL
"""

df = pd.read_sql(query, conn)
conn.close()

print(f"📊 Rows loaded: {len(df)}")

if df.empty:
    raise RuntimeError("No training data available")

# -------------------------
# Feature / target split
# -------------------------
X = df.drop(columns=["race_position"])
y = df["race_position"]

# Keep only numeric columns (SAFE)
X = X.select_dtypes(include=["number"])

# -------------------------
# Train model
# -------------------------
model = RandomForestClassifier(
    n_estimators=300,
    random_state=42,
    n_jobs=-1
)

model.fit(X, y)

# -------------------------
# Save model
# -------------------------
joblib.dump(model, "model.pkl")

print("✅ Model trained & saved as model.pkl")