import streamlit as st
import os
import psycopg2

st.title("⚡ Formula E Winner AI")

# Connect to database
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Create Formula E tables (SAFE – does not touch F1 tables)
cur.execute("""
CREATE TABLE IF NOT EXISTS fe_drivers (
    id SERIAL PRIMARY KEY,
    driver_code TEXT UNIQUE,
    name TEXT
);

CREATE TABLE IF NOT EXISTS fe_teams (
    id SERIAL PRIMARY KEY,
    team_code TEXT UNIQUE,
    name TEXT
);

CREATE TABLE IF NOT EXISTS fe_races (
    id SERIAL PRIMARY KEY,
    race_code TEXT UNIQUE,
    location TEXT,
    season INT
);
""")

conn.commit()

st.success("✅ Formula E database structure ready")

cur.close()
conn.close()
