import streamlit as st
import os
import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

# ---------------- UI SETUP ----------------
st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🤖 F1 Winner AI — Model Training (2025)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

st.write(
    "This step trains a machine learning model to predict race winners "
    "using qualifying performance, driver form, and constructor strength."
)

# ---------------- TRAIN MODEL ----------------
if st.button("Train ML Model"):

    try:
        # Connect to DB
        conn = psycopg2.connect(DATABASE_URL)

        # Load training data
        df = pd.read_sql("""
        SELECT
            qualy_score,
            avg_finish_5,
            avg_team_finish_24,
            winner
        FROM f1_training_data
        WHERE winner IS NOT NULL;
        """, conn)

        conn.close()

        # Guard: empty dataset
        if df.empty:
            st.error("❌ No training data available yet.")
            st.stop()

        # ---------------- HANDLE MISSING VALUES ----------------
        # Driver recent form (early season may be NULL)
        df["avg_finish_5"] = df["avg_finish_5"].fillna(
            df["avg_finish_5"].median()
        )

        # Constructor strength (early season may be NULL)
        df["avg_team_finish_24"] = df["avg_team_finish_24"].fillna(
            df["avg_team_finish_24"].median()
        )

        # Qualifying score is mandatory
        df = df.dropna(subset=["qualy_score"])

        # Guard: not enough rows
        if len(df) < 20:
            st.error("❌ Not enough data to train a model yet.")
            st.write(f"Rows available: {len(df)}")
            st.stop()

        # ---------------- FEATURES / LABEL ----------------
        X = df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
        y = df["winner"]

        # ---------------- TRAIN / TEST SPLIT ----------------
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=y if y.nunique() > 1 else None
        )

        # ---------------- MODEL ----------------
        model = LogisticRegression(max_iter=1000)
        model.fit(X_train, y_train)

        # ---------------- EVALUATION ----------------
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob) if y_test.nunique() > 1 else None

        st.success("✅ Model trained successfully")
        st.write(f"🎯 Accuracy: {accuracy:.2f}")

        if auc:
            st.write(f"📈 ROC-AUC: {auc:.2f}")

        # ---------------- FEATURE IMPORTANCE ----------------
        st.subheader("📊 Feature Importance (Model Weights)")
        for feature, coef in zip(X.columns, model.coef_[0]):
            st.write(f"{feature}: {coef:.4f}")

    except Exception as e:
        st.error("❌ Model training failed")
        st.code(str(e))