import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Winner AI")

# --- Database connection ---
db_url = os.getenv("DATABASE_URL")
conn = psycopg2.connect(db_url)
cur = conn.cursor()

# --- Ensure table exists (old or new) ---
cur.execute("""
CREATE TABLE IF NOT EXISTS f1_drivers (
    id SERIAL PRIMARY KEY,
    driver_id TEXT UNIQUE
);
""")

# --- Safe schema migration (adds columns if missing) ---
cur.execute("""
ALTER TABLE f1_drivers
ADD COLUMN IF NOT EXISTS first_name TEXT,
ADD COLUMN IF NOT EXISTS last_name TEXT,
ADD COLUMN IF NOT EXISTS short_name TEXT,
ADD COLUMN IF NOT EXISTS nationality TEXT,
ADD COLUMN IF NOT EXISTS number INT;
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
            d.get("name"),
            d.get("surname"),
            d.get("shortName"),
            d.get("nationality"),
            d.get("number")
        ))

except Exception:
    drivers = []

# --- Insert / update drivers safely ---
added = 0
for row in drivers:
    cur.execute(
        """
        INSERT INTO f1_drivers
        (driver_id, first_name, last_name, short_name, nationality, number)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT (driver_id)
        DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            short_name = EXCLUDED.short_name,
            nationality = EXCLUDED.nationality,
            number = EXCLUDED.number
        """,
        row
    )
    added += cur.rowcount

conn.commit()

st.success("✅ F1 driver sync complete (schema migrated)")
st.write(f"🔄 Drivers processed this run: {added}")

# --- Display drivers ---
cur.execute("""
SELECT first_name, last_name, short_name, nationality, number
FROM f1_drivers
ORDER BY last_name
""")
rows = cur.fetchall()

st.subheader("Drivers in Database")
for r in rows:
    st.write(f"• {r[0]} {r[1]} ({r[2]}) – #{r[4]} – {r[3]}")

cur.close()
conn.close()
