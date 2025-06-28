import streamlit as st
import pandas as pd
import psycopg2
from datetime import date

st.title("EIA Daily Electricity Consumption Dashboard")

# DB connection
@st.cache_data
def get_data(start_date, end_date):
    conn = psycopg2.connect(
        host="localhost",  # or use your Docker network bridge name
        port="5433",
        database="eia",
        user="postgres",
        password="SJpsql15!@#"
    )
    query = f"""
        SELECT * FROM eia_fuel_type_data
        WHERE period BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY period;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Sidebar filters
start_date = st.sidebar.date_input("Start Date", date(2025, 1, 1))
end_date = st.sidebar.date_input("End Date", date(2025, 2, 1))

if start_date > end_date:
    st.error("Start date must be before end date.")
else:
    data = get_data(start_date, end_date)
    st.line_chart(data.set_index('period')['value'])  # assumes a 'value' column
    st.dataframe(data)
