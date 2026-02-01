import streamlit as st
import os
import psycopg2
import requests
import csv
from io import StringIO

st.title("🏁 F1 Winner AI")

# Connect to database
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Ensure drivers table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);
""")
conn.commit()

# GitHub-hosted F1 drivers dataset (VERY reliable)
csv_url = "https://raw.githubusercontent.com/jokecamp/FootballData/master/data/f1/drivers.csv"

response = requests.get(csv_url, timeout=15)
response.raise_for_status()

csv_file = StringIO(response.text)
reader = csv.DictReader(csv_file)

drivers = []
for row in reader:
    first = row.get("forename")
    last = row.get("surname")
    if first and last:
        drivers.append(f"{first} {last}")

# Auto-sync drivers (no duplicates)
new_drivers = 0
for name in drivers:
    cur.execute(
        "INSERT INTO drivers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (name,)
    )
    new_drivers += cur.rowcount

conn.commit()

st.success("✅ Drivers synced from GitHub dataset")
st.write(f"🆕 New drivers added this run: {new_drivers}")

# Show drivers
cur.execute("SELECT name FROM drivers ORDER BY name")
all_drivers = cur.fetchall()

st.subheader("Current Drivers in Database")
for d in all_drivers:
    st.write("•", d[0])

cur.close()
conn.close()
