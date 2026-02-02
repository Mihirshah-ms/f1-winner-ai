import streamlit as st
import requests

st.title("🔍 F1API.dev Inspector")

url = "https://f1api.dev/api/current/drivers"

try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()
    data = response.json()

    st.success("✅ Successfully fetched data from f1api.dev")
    st.write("Raw response:")
    st.json(data)

except Exception as e:
    st.error("❌ Failed to fetch data from f1api.dev")
    st.write(str(e))
