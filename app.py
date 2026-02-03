import streamlit as st
import os
import psycopg2
import requests

st.title("🏁 F1 Data Importer — 2025 Race Results")

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

if st.button("Import 2025 Race Results"):
    inserted = 0
    skipped = 0

    for rnd in range(1, 25):
        url = f"https://f1api.dev/api/2025/{rnd}/race"

        try:
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()
        except Exception:
            skipped += 1
            continue

        race = data.get("races")
        if not race:
            skipped += 1
            continue

        race_id = race.get("raceId")
        results = race.get("results", [])

        for res in results:
            pos_raw = res.get("position")
            position = None if pos_raw in [None, "-", "R", "DQ"] else int(pos_raw)

            cur.execute("""
            INSERT INTO f1_race_results (
                season,
                round,
                race_id,
                driver_id,
                position
            )
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """, (
                2025,
                rnd,
                race_id,
                res.get("driverId"),
                position
            ))

            inserted += 1

    conn.commit()
    st.success(f"✅ Imported {inserted} race result rows for 2025")
    st.info(f"ℹ️ Skipped rounds: {skipped}")

cur.close()
conn.close()
