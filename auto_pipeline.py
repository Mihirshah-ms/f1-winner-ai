"""
Daily automated pipeline for F1 Winner AI
Runs:
- Race results sync
- Qualifying sync
- Feature rebuild
- Model retrain
"""

import os
import psycopg2
import requests
import pandas as pd
from collections import defaultdict, deque
from sklearn.ensemble import GradientBoostingClassifier

DATABASE_URL = os.getenv("DATABASE_URL")

def run_pipeline():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # ---------------- 1. RACE RESULTS ----------------
    cur.execute("""
    SELECT season, round, race_id
    FROM f1_races
    WHERE race_date <= CURRENT_DATE
    """)
    races = cur.fetchall()

    for season, rnd, race_id in races:
        try:
            url = f"https://f1api.dev/api/{season}/{rnd}/race"
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()

            race = data.get("races", {})
            results = race.get("results", [])

            for res in results:
                driver = res.get("driver", {})
                team = res.get("team", {})

                raw_pos = res.get("position")
                pos = int(raw_pos) if raw_pos and str(raw_pos).isdigit() else None

                cur.execute("""
                INSERT INTO f1_race_results
                (season, round, race_id, driver_id, team_id, position, status)
                VALUES (%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (season, round, driver_id) DO NOTHING
                """, (
                    season, rnd, race_id,
                    driver.get("driverId"),
                    team.get("teamId"),
                    pos,
                    res.get("status")
                ))
            conn.commit()
        except:
            conn.rollback()

    # ---------------- 2. QUALIFYING ----------------
    for season, rnd, race_id in races:
        try:
            url = f"https://f1connectapi.vercel.app/api/{season}/{rnd}/qualy"
            r = requests.get(url, timeout=20)
            r.raise_for_status()
            data = r.json()

            race = data.get("races", {})
            results = race.get("qualyResults", [])

            for res in results:
                raw_pos = res.get("gridPosition")
                pos = int(raw_pos) if str(raw_pos).isdigit() else None

                cur.execute("""
                INSERT INTO f1_qualifying_results
                (season, round, race_id, driver_id, position, q1_time, q2_time, q3_time)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (season, round, driver_id) DO UPDATE SET
                    position = EXCLUDED.position,
                    q1_time = EXCLUDED.q1_time,
                    q2_time = EXCLUDED.q2_time,
                    q3_time = EXCLUDED.q3_time
                """, (
                    season, rnd, race_id,
                    res.get("driverId"),
                    pos,
                    res.get("q1"),
                    res.get("q2"),
                    res.get("q3")
                ))
            conn.commit()
        except:
            conn.rollback()

    # ---------------- 3. RETRAIN MODEL ----------------
    df = pd.read_sql("""
    SELECT qualy_score, constructor_score, avg_driver_form, winner
    FROM f1_training_data
    """, conn)

    X = df[["qualy_score", "constructor_score", "avg_driver_form"]]
    y = df["winner"]

    model = GradientBoostingClassifier(
        n_estimators=200,
        learning_rate=0.05,
        max_depth=3,
        random_state=42
    )
    model.fit(X, y)

    # Save model coefficients (simple persistence)
    cur.execute("DELETE FROM f1_model_meta")
    cur.execute("""
    INSERT INTO f1_model_meta (trained_at, rows_used)
    VALUES (NOW(), %s)
    """, (len(df),))

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_pipeline()
