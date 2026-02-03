import streamlit as st
import os
import psycopg2
import pandas as pd

from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.impute import SimpleImputer

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
        # ---------------- LOAD DATA ----------------
        conn = psycopg2.connect(DATABASE_URL)

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

        if df.empty:
            st.error("❌ No training data available.")
            st.stop()

        # ---------------- FEATURES / LABEL ----------------
        X = df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
        y = df["winner"]

        if y.nunique() < 2:
            st.error("❌ Not enough class variation to train model.")
            st.stop()

        # ---------------- TRAIN / TEST SPLIT ----------------
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=0.2,
            random_state=42,
            stratify=y
        )

        # ---------------- PIPELINE (IMPUTER + MODEL) ----------------
        st.subheader("📊 Feature Importance (Model Coefficients)")

        feature_names = pipeline.named_steps["imputer"].get_feature_names_out(X.columns)
        coefs = pipeline.named_steps["model"].coef_[0]

        for feature, coef in zip(feature_names, coefs):
            st.write(f"{feature}: {coef:.4f}")

        # ---------------- TRAIN ----------------
        pipeline.fit(X_train, y_train)

        # ---------------- EVALUATE ----------------
        y_pred = pipeline.predict(X_test)
        y_prob = pipeline.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob)

        st.success("✅ Model trained successfully")
        st.write(f"🎯 Accuracy: {accuracy:.2f}")
        st.write(f"📈 ROC-AUC: {auc:.2f}")

        # ---------------- FEATURE IMPORTANCE ----------------
        st.subheader("📊 Feature Importance (Model Coefficients)")

        coefs = pipeline.named_steps["model"].coef_[0]
        for feature, coef in zip(X.columns, coefs):
            st.write(f"{feature}: {coef:.4f}")

    except Exception as e:
        st.error("❌ Model training failed")
        st.code(str(e))
