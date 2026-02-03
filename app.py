import streamlit as st
import os
import psycopg2

st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🏎️ F1 Feature Engineering — Driver Recent Form (2025)")

# --- Environment check ---
DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Please check Railway Variables.")
    st.stop()

st.write("This step computes average finishing position for each driver using all previous races in 2025.")

# --- Action button ---
if st.button("Compute Driver Recent Form (2025)"):

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Drop and recreate feature table
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

        # Show sample output
        cur.execute("""
        SELECT driver_id, ROUND(avg_finish_5, 2) AS avg_finish
        FROM f1_driver_recent_form
        ORDER BY avg_finish
        LIMIT 10;
        """)

        rows = cur.fetchall()

        st.success("✅ Driver recent form computed successfully")
        st.subheader("🔥 Drivers in Best Recent Form")

        if rows:
            for row in rows:
                st.write(row)
        else:
            st.warning("No rows found. This usually means only Round 1 exists.")

        cur.close()
        conn.close()

    except Exception as e:
        st.error("❌ Error while computing driver recent form")
        st.code(str(e))