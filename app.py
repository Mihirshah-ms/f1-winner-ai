import streamlit as st
import os
import psycopg2
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score, roc_auc_score

st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🤖 F1 Winner AI — Model Training (2025)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

st.write("This step trains a machine learning model to predict race winners using 2025 data.")

if st.button("Train ML Model"):

    try:
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

        # Drop rows with missing features
        df = df.dropna()

        X = df[["qualy_score", "avg_finish_5", "avg_team_finish_24"]]
        y = df["winner"]

        # Train / test split
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42, stratify=y
        )

        # Train model
        model = LogisticRegression(max_iter=1000)
        model.fit(X_train, y_train)

        # Evaluate
        y_pred = model.predict(X_test)
        y_prob = model.predict_proba(X_test)[:, 1]

        accuracy = accuracy_score(y_test, y_pred)
        auc = roc_auc_score(y_test, y_prob)

        st.success("✅ Model trained successfully")
        st.write(f"🎯 Accuracy: {accuracy:.2f}")
        st.write(f"📈 ROC-AUC: {auc:.2f}")

        # Feature importance
        st.subheader("📊 Feature Importance (Model Weights)")
        for feature, coef in zip(X.columns, model.coef_[0]):
            st.write(f"{feature}: {coef:.4f}")

    except Exception as e:
        st.error("❌ Model training failed")
        st.code(str(e))