import streamlit as st
import requests

st.title("🔍 F1 Qualifying Inspector — Type Safe")

url = "https://f1api.dev/api/current/last/qualy"

try:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()

    st.success("✅ Qualifying data fetched successfully")

    races = data.get("races", [])
    st.write(f"Total races returned: {len(races)}")

    found = False

    for race in races:
        # Skip non-dictionary items
        if not isinstance(race, dict):
            continue

        race_keys = list(race.keys())

        for key in ["qualifyingResults", "qualyResults", "qualifying"]:
            if key in race and isinstance(race[key], list) and len(race[key]) > 0:
                st.subheader("✅ Found race with qualifying data")
                st.write("Race keys:", race_keys)
                st.write("Qualifying key:", key)
                st.subheader("Sample qualifying result object")
                st.json(race[key][0])
                found = True
                break

        if found:
            break

    if not found:
        st.warning("No race with qualifying data found")

except Exception as e:
    st.error("❌ Inspection error")
    st.write(str(e))
