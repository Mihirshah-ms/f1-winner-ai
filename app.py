import streamlit as st
import requests
import csv
from io import StringIO

st.title("🔍 Formula E Driver Source Inspector")

url = "https://raw.githubusercontent.com/f1db/formula-e-db/master/csv/drivers.csv"

try:
    response = requests.get(url, timeout=15)
    response.raise_for_status()

    csv_file = StringIO(response.text)
    reader = csv.DictReader(csv_file)

    rows = list(reader)

    st.success("✅ Successfully fetched Formula E driver data")
    st.write(f"Rows found: {len(rows)}")

    st.subheader("Sample drivers (first 5)")
    for r in rows[:5]:
        st.write(r)

except Exception as e:
    st.error("❌ Failed to fetch Formula E driver data")
    st.write(str(e))
