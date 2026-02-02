import streamlit as st
import requests

st.title("🔍 F1 Qualifying Inspector — Last Session")

url = "https://f1api.dev/api/current/last/qualy"

try:
    response = requests.get(url, timeout=20)
    response.raise_for_status()
    data = response.json()

    st.success("✅ Successfully fetched last qualifying data")
    st.write("Raw response:")
    st.json(data)

    # Show one sample driver object (if exists)
    races = data.get("races", [])
    if races:
        results = races[0].get("qualifyingResults", [])
        if results:
            st.subheader("Sample qualifying result (one driver)")
            st.json(results[0])

except Exception as e:
    st.error("❌ Failed to fetch qualifying data")
    st.write(str(e))
