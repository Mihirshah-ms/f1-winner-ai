import streamlit as st
import os
import psycopg2
import pandas as pd
import pickle

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("DATABASE_URL not set")
    st.stop()

# ✅ 1. CREATE CONNECTION FIRST
conn = psycopg2.connect(DATABASE_URL)

# ✅ 2. CREATE CURSOR
cur = conn.cursor()

# ✅ 3. LOAD MODEL FROM DB
cur.execute("""
SELECT model_blob
FROM ml_models
WHERE model_name = 'f1_winner_model'
LIMIT 1;
""")

row = cur.fetchone()

if not row:
    st.error("❌ Model not trained yet. Wait for cron run.")
    st.stop()

model = pickle.loads(row[0])