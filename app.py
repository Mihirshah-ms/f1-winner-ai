import streamlit as st
import os
import psycopg2

st.title("🏁 F1 Winner AI")

db_url = os.getenv("DATABASE_URL")

conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Create tables automatically
cur.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);

CREATE TABLE IF NOT EXISTS races (
    id SERIAL PRIMARY KEY,
    name TEXT,
    season INT
);
""")

conn.commit()

st.success("✅ Database tables created successfully!")

cur.close()
conn.close()
