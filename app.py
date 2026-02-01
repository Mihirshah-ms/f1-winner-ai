import streamlit as st
import os
import psycopg2

st.title("🏁 F1 Winner AI")

db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# Ensure table exists
cur.execute("""
CREATE TABLE IF NOT EXISTS drivers (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE
);
""")
conn.commit()

# Simulated official F1 driver source (this will be replaced by live API later)
official_drivers = [
    "Max Verstappen",
    "Lewis Hamilton",
    "Charles Leclerc",
    "Lando Norris",
    "Carlos Sainz"
]

# Auto-sync drivers (adds new ones only)
new_drivers = 0
for d in official_drivers:
    cur.execute(
        "INSERT INTO drivers (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
        (d,)
    )
    new_drivers += cur.rowcount

conn.commit()

st.success("✅ Driver list synced automatically")
st.write(f"🆕 New drivers added this run: {new_drivers}")

# Show current drivers
cur.execute("SELECT name FROM drivers ORDER BY name")
drivers = cur.fetchall()

st.subheader("Current Drivers in Database")
for d in drivers:
    st.write("•", d[0])

cur.close()
conn.close()
