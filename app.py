import streamlit as st
import requests

st.title("🔍 jolpica-f1 Driver Data Inspector")

url = "https://api.jolpi.ca/f1/drivers"

try:
    response = requests.get(url, timeout=10)
    response.raise_for_status()
    data = response.json()

    st.success("✅ Successfully fetched data from jolpica-f1")
    st.write("Raw response:")
    st.json(data)

except Exception as e:
    st.error("❌ Failed to fetch data")
    st.write(str(e))
