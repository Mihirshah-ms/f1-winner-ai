import streamlit as st
import os
import psycopg2

st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🧠 F1 ML — Build Training Dataset (2025)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

st.write("This step builds the final ML-ready training dataset using 2025 data.")

if st.button("Build Training Dataset (2025)"):

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Drop old dataset
        cur.execute("""
        DROP TABLE IF EXISTS f1_training_data;
        """)

        # Create training dataset
        cur.execute("""
        CREATE TABLE f1_training_data AS
        SELECT
            q.season,
            q.round,
            q.driver_id,
            q.qualy_score,
            d.avg_finish_5,
            c.avg_team_finish_24,
            r.position AS race_position,
            CASE WHEN r.position = 1 THEN 1 ELSE 0 END AS winner
        FROM f1_qualifying_features q
        JOIN f1_race_results r
            ON q.season = r.season
           AND q.round = r.round
           AND q.driver_id = r.driver_id
        LEFT JOIN f1_driver_recent_form d
            ON q.season = d.season
           AND q.round = d.round
           AND q.driver_id = d.driver_id
        LEFT JOIN f1_constructor_strength c
            ON q.season = c.season
           AND q.round = c.round
           AND r.team_id = c.team_id
        WHERE q.season = 2025
          AND r.position IS NOT NULL;
        """)

        conn.commit()

        # Show dataset stats
        cur.execute("""
        SELECT
            COUNT(*) AS rows,
            SUM(winner) AS winners
        FROM f1_training_data;
        """)

        rows, winners = cur.fetchone()

        st.success("✅ Training dataset built successfully")
        st.write(f"📊 Rows: {rows}")
        st.write(f"🏆 Winning samples: {winners}")

        # Show sample
        cur.execute("""
        SELECT *
        FROM f1_training_data
        LIMIT 5;
        """)

        sample = cur.fetchall()
        st.subheader("🔍 Sample Training Rows")
        for row in sample:
            st.write(row)

        cur.close()
        conn.close()

    except Exception as e:
        st.error("❌ Failed to build training dataset")
        st.code(str(e))