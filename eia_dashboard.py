import streamlit as st
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns
import psycopg2
from datetime import date
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="EIA Electricity Dashboard", layout="wide")
st.title("⚡ EIA Electricity Dashboard")

POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_DB = os.getenv("POSTGRES_DB")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

# Database connection
@st.cache_data
def get_data(query):
    conn = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        database=POSTGRES_DB,
        user=POSTGRES_USER,
        password=str(POSTGRES_PASSWORD) # Replace with your actual password
    )
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Sidebar Filters
st.sidebar.header("📅 Filter Options")
start_date = st.sidebar.date_input("Start Date", date(2025, 1, 1))
end_date = st.sidebar.date_input("End Date", date(2025, 2, 1))

if start_date > end_date:
    st.error("Start date must be before end date.")
    st.stop()

# ---- Load Data ----
fuel_query = f"""
    SELECT * FROM eia_fuel_type_data
    WHERE period BETWEEN '{start_date}' AND '{end_date}';
"""
fuel_data = get_data(fuel_query)

sales_query = f"""
    SELECT * FROM eia_monthly_sales
    WHERE period BETWEEN '{start_date}' AND '{end_date}';
"""
sales_data = get_data(sales_query)

if fuel_data.empty and sales_data.empty:
    st.warning("No data found for selected date range.")
    st.stop()

# ---- Detailed Analysis ----
st.header("📊 Detailed Electricity Data Analysis")

col1, col2 = st.columns(2)

# --- Fuel Type Analysis ---
with col1:
    st.subheader("Fuel Generation Summary")
    selected_fuels = st.multiselect(
        "Select Fuel Types", 
        options=fuel_data['fuel_type'].unique().tolist(),
        default=fuel_data['fuel_type'].unique().tolist()
    )

    if selected_fuels:
        filtered_fuel = fuel_data[fuel_data['fuel_type'].isin(selected_fuels)]
        gen_summary = filtered_fuel.groupby(['period', 'fuel_type'])['value'].sum().reset_index()
        pivoted = gen_summary.pivot(index='period', columns='fuel_type', values='value')

        st.line_chart(pivoted)
        st.dataframe(gen_summary, use_container_width=True)

# --- State-wise Sales Analysis ---
with col2:
    if not sales_data.empty:
        st.subheader("State-wise Electricity Sales")
        selected_states = st.multiselect(
            "Select States",
            options=sales_data['state_desc'].unique().tolist(),
            default=sales_data['state_desc'].unique()[:5].tolist()
        )

        if selected_states:
            filtered_sales = sales_data[sales_data['state_desc'].isin(selected_states)]
            avg_sales = filtered_sales.groupby(['period', 'state_desc'])['sales'].mean().reset_index()
            pivoted_sales = avg_sales.pivot(index='period', columns='state_desc', values='sales')

            st.line_chart(pivoted_sales)
            st.dataframe(avg_sales, use_container_width=True)

# ---- National Summary (Optional Section) ----
if not sales_data.empty:
    st.subheader("📈 National Sales & Generation Trends")

    national_gen = fuel_data.groupby('period')['value'].sum().reset_index(name='total_generation')
    national_sales = sales_data.groupby('period')['sales'].sum().reset_index(name='total_sales')

    merged = pd.merge(national_gen, national_sales, on='period', how='inner')
    st.line_chart(merged.set_index('period'))


import plotly.express as px

# ---- Sector-wise Sales Breakdown (Interactive) ----
if not sales_data.empty:
    st.subheader("🏢 Sector-wise Electricity Sales")

    sectors = sales_data['sector_name'].dropna().unique().tolist()
    selected_sectors = st.multiselect(
        "Select Sectors",
        options=sectors,
        default=sectors
    )

    selected_states_for_sector = st.multiselect(
        "Filter by State (Optional)",
        options=sales_data['state_desc'].unique().tolist()
    )

    sector_filtered = sales_data[sales_data['sector_name'].isin(selected_sectors)]

    if selected_states_for_sector:
        sector_filtered = sector_filtered[sector_filtered['state_desc'].isin(selected_states_for_sector)]

    if sector_filtered.empty:
        st.warning("No matching sector data found for selection.")
    else:
        st.markdown("### 📊 Interactive Sector-wise Electricity Sales")

        sector_grouped = (
            sector_filtered
            .groupby(['period', 'sector_name'])['sales']
            .sum()
            .reset_index()
        )

        fig = px.bar(
            sector_grouped,
            x="period",
            y="sales",
            color="sector_name",
            barmode="stack",
            title="Electricity Sales by Sector (Stacked)",
            labels={"sales": "Sales (MWh)", "period": "Period", "sector_name": "Sector"},
            height=500
        )

        fig.update_layout(xaxis_title="Period", yaxis_title="Electricity Sales (MWh)")
        st.plotly_chart(fig, use_container_width=True)


