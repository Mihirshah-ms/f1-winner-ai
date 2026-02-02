import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI")

# Connect to database
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Ensure drivers table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    driver_id TEXT UNIQUE,
    name TEXT
);
""")
conn.commit()

# Try Ergast (stable source)
drivers = []
try:
    url = "https://ergast.com/api/f1/current/drivers.json"
    response = requests.get(url, timeout=10)
    response.raise_for_status()

    data = response.json()
    drivers = [
        (d["driverId"], f"{d['givenName']} {d['familyName']}")
        for d in data["MRData"]["DriverTable"]["Drivers"]
    ]
except Exception:
    # Silent fallback (do nothing)
    drivers = []

# Insert drivers safely
added = 0
for driver_id, name in drivers:
    cur.execute(
        """
        INSERT INTO drivers (driver_id, name)
        VALUES (%s, %s)
        ON CONFLICT (driver_id) DO NOTHING
        """,
        (driver_id, name)
    )
    added += cur.rowcount

conn.commit()

st.success("✅ Ergast driver sync complete")
st.write(f"🆕 New drivers added this run: {added}")

# Show drivers
cur.execute("SELECT name FROM drivers ORDER BY name")
rows = cur.fetchall()

st.subheader("Drivers in Database")
for r in rows:
    st.write("•", r[0])

cur.close()
conn.close()
