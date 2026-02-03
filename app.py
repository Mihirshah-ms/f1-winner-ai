import streamlit as st
import os
import psycopg2
import requests

st.title("📥 F1 Data Importer — 2025 Races")

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

if st.button("Import 2025 Races"):
    url = "https://f1api.dev/api/2025"
    r = requests.get(url, timeout=20)
    r.raise_for_status()
    data = r.json()

    races = data.get("races", [])
    inserted = 0

    for race in races:
        circuit = race.get("circuit", {})

        cur.execute("""
        INSERT INTO f1_races (
            race_id,
            season,
            round,
            race_name,
            race_date,
            race_time,
            circuit_name,
            circuit_country
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (season, round) DO NOTHING
        """, (
            race.get("raceId"),
            2025,
            race.get("round"),
            race.get("raceName"),
            race.get("date"),
            race.get("time"),
            circuit.get("circuitName"),
            circuit.get("country")
        ))

        inserted += cur.rowcount

    conn.commit()
    st.success(f"✅ Imported {inserted} races for 2025")

cur.close()
conn.close()