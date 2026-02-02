import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI")

# --- Database connection ---
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# --- Create drivers table ---
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_drivers (
    id SERIAL PRIMARY KEY,
    driver_id TEXT UNIQUE,
    name TEXT,
    code TEXT
);
""")
conn.commit()

# --- Fetch drivers from f1api.dev ---
drivers = []
try:
    url = "https://f1api.dev/api/current/drivers"
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    data = response.json()

    for d in data.get("drivers", []):
        drivers.append((
            d.get("driverId"),
            f"{d.get('givenName')} {d.get('familyName')}",
            d.get("code")
        ))

except Exception:
    drivers = []

# --- Insert drivers safely ---
added = 0
for driver_id, name, code in drivers:
    cur.execute(
        """
        INSERT INTO f1_drivers (driver_id, name, code)
        VALUES (%s, %s, %s)
        ON CONFLICT (driver_id) DO NOTHING
        """,
        (driver_id, name, code)
    )
    added += cur.rowcount

conn.commit()

st.success("✅ F1 driver sync complete")
st.write(f"🆕 New drivers added this run: {added}")

# --- Display drivers ---
cur.execute("SELECT name, code FROM f1_drivers ORDER BY name")
rows = cur.fetchall()

st.subheader("Drivers in Database")
for name, code in rows:
    st.write(f"• {name} ({code})")

cur.close()
conn.close()
