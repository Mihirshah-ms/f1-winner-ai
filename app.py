import streamlit as st
import os
import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingClassifier
from sklearn.metrics import accuracy_score

st.title("🤖 F1 Winner AI — Model Training")

# ---------------- LOAD DATA ----------------
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)

df = pd.read_sql("""
SELECT
    qualy_score,
    constructor_score,
    avg_driver_form,
    winner
FROM f1_training_data
""", conn)

conn.close()

st.write("Training rows:", len(df))

# ---------------- PREPARE DATA ----------------
X = df[["qualy_score", "constructor_score", "avg_driver_form"]]
y = df["winner"]

X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.25, random_state=42, stratify=y
)

# ---------------- TRAIN MODEL ----------------
model = GradientBoostingClassifier(
    n_estimators=200,
    learning_rate=0.05,
    max_depth=3,
    random_state=42
)

model.fit(X_train, y_train)

# ---------------- EVALUATE ----------------
preds = model.predict(X_test)
acc = accuracy_score(y_test, preds)

st.success(f"✅ Model trained — Accuracy: {acc:.2f}")

# ---------------- FEATURE IMPORTANCE ----------------
st.subheader("Feature Importance")
for name, importance in zip(X.columns, model.feature_importances_):
    st.write(f"{name}: {importance:.3f}")

# ---------------- PREDICT SAMPLE ----------------
df["win_probability"] = model.predict_proba(X)[:, 1]

st.subheader("Top Winner Probabilities (Sample)")
st.dataframe(
    df.sort_values("win_probability", ascending=False).head(10)
)
