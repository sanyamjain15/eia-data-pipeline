# EIA Data Pipeline

This project is an end-to-end data engineering pipeline that ingests and processes U.S. electricity data from the [EIA (U.S. Energy Information Administration)](https://www.eia.gov/) API.

## 🚀 Project Overview

This pipeline automates the ingestion, validation, storage, and reporting of two key datasets from EIA:
- **Daily Fuel-Type Generation Data**  
- **Monthly Retail Electricity Sales Data**

It uses **Apache Airflow** for orchestration, **PostgreSQL** for storage, and includes robust **data quality checks**, alerting, and daily reporting via email.

## 🎯 Why This Project?

Energy is a critical and fast-evolving sector. Clean, reliable, and real-time data is essential for:
- Grid planning
- Renewables integration
- Market transparency
- Energy policy and research

This project:
- Demonstrates real-world data engineering practices
- Automates daily workflows with recovery and alerting
- Emphasizes data quality and trust
- Can scale to include transformations (dbt), cloud deployment (AWS), and dashboards (Streamlit)

## ⚙️ Tech Stack

| Tool         | Purpose                          |
|--------------|----------------------------------|
| Apache Airflow | Orchestration & scheduling     |
| Python        | Data fetching and validation    |
| PostgreSQL    | Raw and processed data storage  |
| Pandas        | Lightweight data wrangling      |
| Email Alerts  | Monitoring & reporting          |
| Docker        | Local environment & portability |

## 📊 Data Sources

1. **Fuel Type Generation**  
   - Endpoint: `/v2/electricity/rto/daily-fuel-type-data`  
   - Granularity: Daily, by fuel type and respondent (ISO)

2. **Retail Sales Data**  
   - Endpoint: `/v2/electricity/retail-sales/data`  
   - Granularity: Monthly, by state and customer sector

## ✅ Data Quality Checks

- **Pre-ingestion**
  - Schema and data type validation
  - Null checks (on identifiers only)
  - Volume thresholds
  - Uniqueness constraints

- **Post-ingestion**
  - SQL-based checks on inserted data
  - Email report summarizing check status

## 🛠️ Features

- Retry logic and error handling
- Pre/post-ingestion quality validation
- Modular Airflow DAGs
- Email alerts with success/failure details
- Easily extensible to add new datasets

## 📁 Project Structure

```plaintext
.
├── dags/
│   └── fetch_eia_final.py
├── data/                      # Raw JSON files
├── logs/                      # Airflow task logs
├── requirements.txt
└── README.md
