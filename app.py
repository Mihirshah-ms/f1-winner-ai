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

# Fetch REAL F1 drivers from official public data
url = "https://ergast.com/api/f1/current/drivers.json"
response = requests.get(url)
data = response.json()

drivers = data["MRData"]["DriverTable"]["Drivers"]

new_drivers = 0
for d in drivers:
    name = f"{d['givenName']} {d['familyName']}"
    cur.execute(
        "INSERT INTO drivers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (name,)
    )
    new_drivers += cur.rowcount

conn.commit()

st.success("✅ Real F1 drivers synced automatically")
st.write(f"🆕 New drivers added this run: {new_drivers}")

# Show current drivers
cur.execute("SELECT name FROM drivers ORDER BY name")
all_drivers = cur.fetchall()

st.subheader("Current Drivers in Database")
for d in all_drivers:
    st.write("•", d[0])

cur.close()
conn.close()
