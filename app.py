import streamlit as st
import os
import psycopg2

st.title("🏁 F1 Winner AI")

db_url = os.getenv("DATABASE_URL")

if not db_url:
    st.error("Database not connected")
else:
    try:
        conn = psycopg2.connect(db_url)
        st.success("✅ Database connected successfully!")
        conn.close()
    except Exception as e:
        st.error("❌ Database connection failed")
