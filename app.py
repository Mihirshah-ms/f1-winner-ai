import streamlit as st
import requests

st.title("🔍 F1 Qualifying Inspector — Safe Mode")

url = "https://f1api.dev/api/current/last/qualy"

try:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()

    st.success("✅ Qualifying data fetched successfully")

    st.subheader("Top-level keys")
    st.write(list(data.keys()))

    if "races" in data:
        st.subheader("Number of races returned")
        st.write(len(data["races"]))

        if len(data["races"]) > 0:
            race = data["races"][0]
            st.subheader("Keys inside first race object")
            st.write(list(race.keys()))

            # Try multiple possible result keys
            for key in ["qualifyingResults", "qualyResults", "results"]:
                if key in race:
                    st.subheader(f"Found qualifying results under key: {key}")
                    results = race[key]
                    if len(results) > 0:
                        st.subheader("Sample qualifying result object")
                        st.json(results[0])
                    break
            else:
                st.warning("No qualifying results array found in race object")

    st.subheader("Full raw JSON (for reference)")
    st.json(data)

except Exception as e:
    st.error("❌ Unexpected error during inspection")
    st.write(str(e))
