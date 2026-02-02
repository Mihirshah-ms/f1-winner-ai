import streamlit as st
import requests

st.title("🔍 F1 Qualifying Inspector — Finder Mode")

url = "https://f1api.dev/api/current/last/qualy"

try:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()

    st.success("✅ Qualifying data fetched successfully")

    races = data.get("races", [])
    st.write(f"Races returned: {len(races)}")

    found = False

    for race in races:
        # Show race metadata safely
        race_keys = list(race.keys())

        # Try known qualifying keys
        for key in ["qualifyingResults", "qualyResults", "qualifying"]:
            if key in race and race[key]:
                st.subheader("✅ Found race with qualifying data")
                st.write("Race keys:", race_keys)
                st.write("Qualifying key:", key)
                st.subheader("Sample qualifying result")
                st.json(race[key][0])
                found = True
                break

        if found:
            break

    if not found:
        st.warning("No race with qualifying data found in this response")

except Exception as e:
    st.error("❌ Inspection error")
    st.write(str(e))
