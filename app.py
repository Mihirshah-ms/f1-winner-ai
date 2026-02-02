import streamlit as st
import os
import requests

st.title("🔍 F1 Live Pulse API Inspector")

api_key = os.getenv("RAPIDAPI_KEY")

if not api_key:
    st.error("❌ RAPIDAPI_KEY not found")
    st.stop()

url = "https://f1-live-pulse.p.rapidapi.com/fiaDocuments"

headers = {
    "x-rapidapi-key": api_key,
    "x-rapidapi-host": "f1-live-pulse.p.rapidapi.com"
}

try:
    response = requests.get(url, headers=headers, timeout=15)
    response.raise_for_status()

    data = response.json()

    st.success("✅ Successfully fetched FIA documents")
    st.write("Raw response:")
    st.json(data)

except Exception as e:
    st.error("❌ Failed to fetch FIA documents")
    st.write(str(e))
