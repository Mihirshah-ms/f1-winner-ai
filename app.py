import streamlit as st
import os
import psycopg2

st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("🏗️ F1 Feature Engineering — Constructor Strength (2025)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

st.write(
    "This step computes constructor strength as the average finishing position "
    "of a team's drivers using all previous races in the season."
)

if st.button("Compute Constructor Strength (2025)"):

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Drop old table if exists
        cur.execute("""
        DROP TABLE IF EXISTS f1_constructor_strength;
        """)

        # Create constructor strength table
        cur.execute("""
        CREATE TABLE f1_constructor_strength AS
        SELECT
            r1.season,
            r1.round,
            r1.team_id,
            AVG(r2.position) AS avg_team_finish_24
        FROM f1_race_results r1
        JOIN f1_race_results r2
            ON r1.team_id = r2.team_id
           AND r2.season = r1.season
           AND r2.round < r1.round
        WHERE r1.season = 2025
          AND r2.position IS NOT NULL
          AND r1.team_id IS NOT NULL
        GROUP BY r1.season, r1.round, r1.team_id;
        """)

        conn.commit()

        # Show sample output
        cur.execute("""
        SELECT team_id, ROUND(avg_team_finish_24, 2) AS strength
        FROM f1_constructor_strength
        ORDER BY strength
        LIMIT 10;
        """)

        rows = cur.fetchall()

        st.success("✅ Constructor strength computed successfully")
        st.subheader("🏆 Strongest Constructors (Sample)")

        if rows:
            for row in rows:
                st.write(row)
        else:
            st.warning("No constructor strength rows found.")

        cur.close()
        conn.close()

    except Exception as e:
        st.error("❌ Error computing constructor strength")
        st.code(str(e))