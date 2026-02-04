import os
import psycopg2
from flask import Flask, render_template

# -----------------------------
# Config
# -----------------------------
DATABASE_URL = os.environ.get("DATABASE_URL")

TARGET_SEASON = 2026

# -----------------------------
# App & DB
# -----------------------------
app = Flask(__name__)

conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = True
cur = conn.cursor()

# -----------------------------
# Helpers
# -----------------------------
def get_upcoming_race_2026():
    """
    Returns the next race in 2026 that has no race result yet
    """
    cur.execute("""
        SELECT
            r.season,
            r.round,
            r.race_name,
            r.race_date,
            r.race_time,
            r.circuit_name,
            r.circuit_country
        FROM f1_races r
        LEFT JOIN f1_race_results rr
          ON r.season = rr.season
         AND r.round = rr.round
        WHERE r.season = %s
          AND rr.season IS NULL
        ORDER BY r.round ASC
        LIMIT 1;
    """, (TARGET_SEASON,))

    return cur.fetchone()


def get_latest_completed_race_2026():
    """
    Returns latest completed race in 2026 (if any)
    """
    cur.execute("""
        SELECT
            r.season,
            r.round,
            r.race_name,
            r.race_date,
            r.circuit_name,
            r.circuit_country
        FROM f1_races r
        JOIN f1_race_results rr
          ON r.season = rr.season
         AND r.round = rr.round
        WHERE r.season = %s
        ORDER BY r.round DESC
        LIMIT 1;
    """, (TARGET_SEASON,))

    return cur.fetchone()

# -----------------------------
# Routes
# -----------------------------
@app.route("/")
def home():
    upcoming = get_upcoming_race_2026()
    latest = get_latest_completed_race_2026()

    # Debug visibility (Railway logs)
    print("UPCOMING 2026 RACE:", upcoming)
    print("LATEST COMPLETED 2026 RACE:", latest)

    return render_template(
        "index.html",
        upcoming_race=upcoming,
        latest_race=latest,
        season=TARGET_SEASON
    )


# -----------------------------
# Health Check
# -----------------------------
@app.route("/health")
def health():
    return {"status": "ok", "season": TARGET_SEASON}


# -----------------------------
# Entry
# -----------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)