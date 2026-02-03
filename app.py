import streamlit as st
import os
import psycopg2

st.title("🏎️ F1 Feature Engineering — Driver Recent Form (Last 5 Races)")

DATABASE_URL = os.getenv("DATABASE_URL")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

if st.button("Compute Driver Recent Form (2025)"):

    cur.execute("""
    DROP TABLE IF EXISTS f1_driver_recent_form;
    """)

    cur.execute("""
    CREATE TABLE f1_driver_recent_form AS
    SELECT
        r1.season,
        r1.round,
        r1.driver_id,
        AVG(r2.position) AS avg_finish_5
    FROM f1_race_results r1
    JOIN f1_race_results r2
        ON r1.driver_id = r2.driver_id
       AND r2.season = r1.season
       AND r2.round < r1.round
    WHERE r1.season = 2025
      AND r2.position IS NOT NULL
    GROUP BY r1.season, r1.round, r1.driver_id;
    """)

    conn.commit()

    cur.execute("""
    SELECT driver_id, avg_finish_5
    FROM f1_driver_recent_form
    ORDER BY avg_finish_5
    LIMIT 10;
    """)

    rows = cur.fetchall()

    st.success("✅ Driver recent form computed (last 5 races)")
    st.write("🏁 Drivers in best recent form:")
    for row in rows:
        st.write(row)

cur.close()
conn.close()
