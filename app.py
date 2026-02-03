import streamlit as st
import os
import psycopg2

st.set_page_config(page_title="F1 Winner AI", layout="centered")
st.title("⏱️ F1 Feature Engineering — Qualifying Performance (2025)")

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    st.error("❌ DATABASE_URL not set. Check Railway Variables.")
    st.stop()

st.write(
    "This step computes a weighted qualifying performance score "
    "using Q1, Q2, and Q3 lap times."
)

def time_to_seconds(t):
    if t is None:
        return None
    try:
        mins, secs = t.split(":")
        return int(mins) * 60 + float(secs)
    except Exception:
        return None

if st.button("Compute Qualifying Performance (2025)"):

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        # Drop old feature table
        cur.execute("""
        DROP TABLE IF EXISTS f1_qualifying_features;
        """)

        # Create feature table
        cur.execute("""
        CREATE TABLE f1_qualifying_features (
            season INT,
            round INT,
            race_id TEXT,
            driver_id TEXT,
            qualy_score FLOAT
        );
        """)

        # Fetch qualifying data
        cur.execute("""
        SELECT season, round, race_id, driver_id, q1_time, q2_time, q3_time
        FROM f1_qualifying_results
        WHERE season = 2025;
        """)

        rows = cur.fetchall()
        inserted = 0

        for season, rnd, race_id, driver_id, q1, q2, q3 in rows:
            q1_s = time_to_seconds(q1)
            q2_s = time_to_seconds(q2)
            q3_s = time_to_seconds(q3)

            weights = []
            values = []

            if q1_s is not None:
                weights.append(0.2)
                values.append(q1_s)
            if q2_s is not None:
                weights.append(0.3)
                values.append(q2_s)
            if q3_s is not None:
                weights.append(0.5)
                values.append(q3_s)

            if not values:
                continue

            qualy_score = sum(w * v for w, v in zip(weights, values)) / sum(weights)

            cur.execute(
                """
                INSERT INTO f1_qualifying_features
                (season, round, race_id, driver_id, qualy_score)
                VALUES (%s,%s,%s,%s,%s)
                """,
                (season, rnd, race_id, driver_id, qualy_score)
            )

            inserted += 1

        conn.commit()

        # Show sample
        cur.execute("""
        SELECT
            driver_id,
            ROUND(qualy_score::numeric, 3) AS score
        FROM f1_qualifying_features
        ORDER BY score
        LIMIT 10;
        """)

        sample = cur.fetchall()

        st.success(f"✅ Qualifying performance computed ({inserted} rows)")
        st.subheader("🏁 Best Qualifying Performances (Sample)")

        for row in sample:
            st.write(row)

        cur.close()
        conn.close()

    except Exception as e:
        st.error("❌ Error computing qualifying performance")
        st.code(str(e))
