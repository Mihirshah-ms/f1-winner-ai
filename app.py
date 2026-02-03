import streamlit as st
import os
import psycopg2
import requests

st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🏁 F1 Data Importer — 2025 Race Results (FIXED)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

if st.button("Re-import 2025 Race Results (FIXED)"):

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # 1️⃣ Clean existing broken data
        cur.execute("""
        DELETE FROM f1_race_results
        WHERE season = 2025;
        """)

        inserted = 0

        # 2️⃣ Re-import correctly
        for rnd in range(1, 25):
            url = f"https://f1api.dev/api/2025/{rnd}/race"

            try:
                r = requests.get(url, timeout=20)
                r.raise_for_status()
                data = r.json()
            except Exception:
                continue

            race = data.get("races")
            if not race:
                continue

            race_id = race.get("raceId")
            results = race.get("results", [])

            for res in results:
                driver_id = res.get("driver", {}).get("driverId")
                team_id = res.get("team", {}).get("teamId")
                status = res.get("status")

                pos_raw = res.get("position")
                position = (
                    None if pos_raw in [None, "-", "R", "DQ", "NC", "DNS", "DNF"]
                    else int(pos_raw)
                )

                cur.execute(
                    """
                    INSERT INTO f1_race_results (
                        season,
                        round,
                        race_id,
                        driver_id,
                        team_id,
                        position,
                        status
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        2025,
                        rnd,
                        race_id,
                        driver_id,
                        team_id,
                        position,
                        status
                    )
                )

                inserted += 1

        conn.commit()

        st.success(f"✅ Re-imported {inserted} race result rows for 2025")
        st.info("Driver ID, Team ID, and Status are now correctly populated.")

        cur.close()
        conn.close()

    except Exception as e:
        st.error("❌ Import failed")
        st.code(str(e))