import streamlit as st
import os
import psycopg2
import requests

st.title("📥 F1 Data Importer — 2025 Qualifying")

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

if st.button("Import 2025 Qualifying"):
    inserted = 0
    skipped = 0

    # 2025 season has 24 races
    for rnd in range(1, 25):
        url = f"https://f1api.dev/api/2025/{rnd}/qualy"

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
        results = race.get("qualyResults", [])

        for res in results:
            cur.execute("""
            INSERT INTO f1_qualifying_results (
                season,
                round,
                race_id,
                driver_id,
                position,
                q1_time,
                q2_time,
                q3_time
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                2025,
                rnd,
                race_id,
                res.get("driverId"),
                None if res.get("gridPosition") == "-" else int(res.get("gridPosition")),
                res.get("q1"),
                res.get("q2"),
                res.get("q3")
            ))

            inserted += 1

    conn.commit()
    st.success(f"✅ Imported {inserted} qualifying rows for 2025")
    st.info(f"ℹ️ Skipped rounds: {skipped}")

cur.close()
conn.close()
