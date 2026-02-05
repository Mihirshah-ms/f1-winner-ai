import psycopg2
import streamlit as st
import time

@st.cache_resource(show_spinner="Connecting to database...")
def get_conn():
    try:
        conn = psycopg2.connect(
            host=st.secrets["DB_HOST"],
            port=int(st.secrets["DB_PORT"]),
            dbname=st.secrets["DB_NAME"],
            user=st.secrets["DB_USER"],
            password=st.secrets["DB_PASSWORD"],
            sslmode="require",
            connect_timeout=5,     # ⬅️ CRITICAL
            keepalives=1,
            keepalives_idle=30,
            keepalives_interval=10,
            keepalives_count=5
        )
        return conn

    except psycopg2.OperationalError as e:
        st.error("❌ Database connection failed")
        st.code(str(e))
        st.stop()

conn = get_conn()
cur = conn.cursor()
cur.execute("SELECT 1;")
st.success("✅ Database connected successfully")

# ----------------------------------------------------
# Page config
# ----------------------------------------------------
st.set_page_config(
    page_title="F1 Winner AI",
    layout="wide"
)

st.title("🏎️ F1 Winner AI")
st.caption("Read-only analytics • 2026 live season")

# ----------------------------------------------------
# Database connection (cached)
# ----------------------------------------------------
@st.cache_resource
def get_conn():
    return psycopg2.connect(
        host=st.secrets["DB_HOST"],
        port=st.secrets["DB_PORT"],
        dbname=st.secrets["DB_NAME"],
        user=st.secrets["DB_USER"],
        password=st.secrets["DB_PASSWORD"],
        sslmode="require"
    )

conn = get_conn()

# ----------------------------------------------------
# Helper queries (lightweight, safe)
# ----------------------------------------------------
@st.cache_data(ttl=300)
def get_upcoming_2026_race():
    query = """
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
        WHERE r.season = 2026
          AND rr.season IS NULL
        ORDER BY r.round ASC
        LIMIT 1;
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=300)
def get_recent_results():
    query = """
        SELECT
            season,
            round,
            race_name,
            race_date
        FROM f1_races
        WHERE season >= 2024
        ORDER BY season DESC, round DESC
        LIMIT 10;
    """
    return pd.read_sql(query, conn)

# ----------------------------------------------------
# UI sections
# ----------------------------------------------------
st.subheader("🏁 Upcoming Race (2026)")

upcoming = get_upcoming_2026_race()

if upcoming.empty:
    st.info("No upcoming 2026 races found yet.")
else:
    r = upcoming.iloc[0]

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Season", r["season"])
        st.metric("Round", r["round"])

    with col2:
        st.metric("Race", r["race_name"])
        st.metric("Circuit", r["circuit_name"])

    with col3:
        st.metric("Country", r["circuit_country"])
        st.metric("Race Date", str(r["race_date"]))

# ----------------------------------------------------
st.divider()

st.subheader("📊 Recent Races (Read-only)")

recent = get_recent_results()

if recent.empty:
    st.warning("No race data available.")
else:
    st.dataframe(
        recent,
        use_container_width=True,
        hide_index=True
    )

# ----------------------------------------------------
st.divider()

st.subheader("🤖 ML Model Status")

st.info(
    "Model training is handled by the auto-pipeline.\n\n"
    "Training activates automatically once:\n"
    "• Qualifying exists\n"
    "• Race results exist\n"
    "• At least one 2026 race is completed"
)

# ----------------------------------------------------
st.caption("F1 Analytics Platform • Streamlit Cloud • Safe Mode Enabled")
