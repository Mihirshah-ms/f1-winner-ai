import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI")

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

# Try fetching real F1 drivers (SAFE)
try:
    url = "https://ergast.com/api/f1/current/drivers.json"
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()
    drivers = [
        f"{d['givenName']} {d['familyName']}"
        for d in data["MRData"]["DriverTable"]["Drivers"]
    ]
    source = "Live F1 data"
except Exception:
    # Fallback if internet/API fails
    drivers = [
        "Max Verstappen",
        "Lewis Hamilton",
        "Charles Leclerc",
        "Lando Norris",
        "Carlos Sainz"
    ]
    source = "Fallback cache (API unavailable)"

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
st.info(f"📡 Data source: {source}")
st.write(f"🆕 New drivers added this run: {new_drivers}")

# Show drivers
cur.execute("SELECT name FROM drivers ORDER BY name")
all_drivers = cur.fetchall()

st.subheader("Current Drivers in Database")
for d in all_drivers:
    st.write("•", d[0])

cur.close()
conn.close()
