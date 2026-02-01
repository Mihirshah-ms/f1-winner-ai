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

drivers = []
source = ""

# Reliable GitHub-hosted F1 drivers dataset (VERIFIED)
csv_url = "https://raw.githubusercontent.com/ergast/f1db/master/csv/drivers.csv"

try:
    response = requests.get(csv_url, timeout=15)
    response.raise_for_status()

    csv_file = StringIO(response.text)
    reader = csv.DictReader(csv_file)

    for row in reader:
        first = row.get("forename")
        last = row.get("surname")
        if first and last:
            drivers.append(f"{first} {last}")

    source = "GitHub F1 dataset (stable)"

except Exception:
    # Absolute last-resort fallback (never crashes)
    drivers = [
        "Max Verstappen",
        "Lewis Hamilton",
        "Charles Leclerc",
        "Lando Norris",
        "Carlos Sainz"
    ]
    source = "Emergency fallback"

# Auto-sync drivers safely
new_drivers = 0
for name in drivers:
    cur.execute(
        "INSERT INTO drivers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (name,)
    )
    new_drivers += cur.rowcount

conn.commit()

st.success("✅ Driver sync completed")
st.info(f"📦 Data source: {source}")
st.write(f"🆕 New drivers added this run: {new_drivers}")

# Show drivers
cur.execute("SELECT name FROM drivers ORDER BY name")
all_drivers = cur.fetchall()

st.subheader("Current Drivers in Database")
for d in all_drivers:
    st.write("•", d[0])

cur.close()
conn.close()
