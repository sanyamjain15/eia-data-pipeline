from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowException
import pandas as pd
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import requests
import json
import os

# Config
EIA_API_KEY = os.getenv("EIA_API_KEY")
EMAIL_ID = os.getenv("SMTP_USER")
DATA_DIR = "/opt/airflow/data"

def failure_email_alert(context):
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url
    subject = f"[Airflow Alert] DAG {dag_id} Task {task_id} Failed"
    html_content = f"""
        Task {task_id} in DAG {dag_id} failed on {execution_date}. <br>
        <a href="{log_url}">Log link</a>
    """
    send_email(to=[str(EMAIL_ID)], subject=subject, html_content=html_content)

def render_success_email(**context):
    dq_report = context['ti'].xcom_pull(task_ids='post_ingestion_dq_check', key='dq_report') or "No DQ report available."

    return f"""
    <h3>DAG succeeded!</h3>
    <p>Your DAG <strong>eia_daily_ingestion</strong> ran successfully on <strong>{context['ds']}</strong>.</p>
    <h4>Data Quality Report</h4>
    {dq_report}
    """

def notify_success(**context):
    html_content = render_success_email(**context)
    send_email(
        to=[str(EMAIL_ID)],
        subject=f"Airflow DAG Succeeded: eia_daily_ingestion ({context['ds']})",
        html_content=html_content
    )

def fetch_eia_fuel_data(**context):
    execution_date = context['execution_date']
    target_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")
    url = (
        f"https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"
        f"?api_key={EIA_API_KEY}"
        f"&frequency=daily"
        f"&data[0]=value"
        f"&start={target_date}&end={target_date}"
        f"&sort[0][column]=period&sort[0][direction]=desc"
        f"&offset=0&length=5000"
    )
    
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch data: {response.status_code}, {response.text}")

    data = response.json()

    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, f"eia_data_{target_date}.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)
    
    context['ti'].xcom_push(key='file_path', value=file_path)
    context['ti'].xcom_push(key='target_date', value=target_date)
    print(f"Saved EIA data to {file_path}")

def run_pre_ingestion_dq_fuel(**context):
    file_path = context['ti'].xcom_pull(key='file_path')
    if not file_path or not os.path.exists(file_path):
        raise AirflowException("Pre-ingestion DQ failed: file not found.")

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        raise AirflowException("Pre-ingestion DQ failed: no data in file.")

    df = pd.DataFrame(records)

    expected_schema = {
        "period": str,
        "respondent": str,
        "respondent-name": str,
        "fueltype": str,
        "type-name": str,
        "timezone": str,
        "value": (int, float),
        "value-units": str
    }

    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            raise AirflowException(f"Missing column in schema: {col}")
    
        try:
            if expected_type in [int, float, (int, float)]:
                df[col] = pd.to_numeric(df[col], errors='raise')
            else:
                df[col] = df[col].astype(str)
        except Exception as e:
            raise AirflowException(f"Failed to convert column {col} to {expected_type}: {str(e)}")

    essential_cols = ["period", "respondent", "fueltype", "value"]
    for col in essential_cols:
        if df[col].isnull().any():
            raise AirflowException(f"Null values found in {col}")

    if df.duplicated(subset=["period", "respondent", "fueltype", "timezone"]).any():
        raise AirflowException("Duplicate rows found based on PK subset")

    row_count = len(df)
    if row_count < 100 or row_count > 10000:
        raise AirflowException(f"Volume check failed: {row_count} rows found")

def load_eia_fuel_data(**context):
    from psycopg2 import sql

    file_path = context['ti'].xcom_pull(key='file_path')
    target_date = context['ti'].xcom_pull(key='target_date')

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        print(f"No data found for {target_date}")
        return

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # 1. Create table if not exists
    create_table_sql = """
        CREATE TABLE IF NOT EXISTS eia_fuel_type_data (
            period DATE,
            respondent TEXT,
            respondent_name TEXT,
            fuel_type TEXT,
            type_name TEXT,
            timezone TEXT,
            value NUMERIC,
            value_units TEXT,
            PRIMARY KEY (period, respondent, fuel_type, timezone)
        );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # 2. Insert records
    insert_sql = """
        INSERT INTO eia_fuel_type_data (period, respondent, respondent_name, fuel_type, type_name, timezone, value, value_units)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (period, respondent, fuel_type, timezone)
        DO NOTHING;
    """

    rows = [
        (r["period"], r["respondent"], r["respondent-name"], r["fueltype"], r["type-name"], r["timezone"], r["value"], r["value-units"])
        for r in records
    ]

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Inserted {len(rows)} rows into Postgres.")

def fetch_eia_sales_data(**context):
    url = (
        "https://api.eia.gov/v2/electricity/retail-sales/data/"
        f"?api_key={EIA_API_KEY}"
        "&frequency=monthly"
        "&data[0]=customers"
        "&data[1]=price"
        "&data[2]=revenue"
        "&data[3]=sales"
        "&sort[0][column]=period&sort[0][direction]=desc"
        "&offset=0&length=5000"
        f"&start=2025-01"
    )

    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch monthly sales data: {response.status_code}, {response.text}")

    data = response.json()
    file_path = os.path.join(DATA_DIR, f"eia_sales_data.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    context['ti'].xcom_push(key='sales_file_path', value=file_path)

def run_pre_ingestion_dq_sales(**context):
    file_path = context['ti'].xcom_pull(key='sales_file_path')
    if not file_path or not os.path.exists(file_path):
        raise AirflowException("Pre-ingestion DQ failed: sales file not found.")

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        raise AirflowException("Pre-ingestion DQ failed: no sales data in file.")

    df = pd.DataFrame(records)

    # 1. Schema & data types
    expected_schema = {
        "period": str,
        "stateid": str,
        "stateDescription": str,
        "sectorid": str,
        "sectorName": str,
        "customers": (int, float),
        "price": (int, float),
        "revenue": (int, float),
        "sales": (int, float),
        "customers-units": str,
        "price-units": str,
        "revenue-units": str,
        "sales-units": str,
    }

    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            raise AirflowException(f"[Sales DQ] Missing column: {col}")

        try:
            if expected_type in [int, float, (int, float)]:
                df[col] = pd.to_numeric(df[col], errors='raise')
            else:
                df[col] = df[col].astype(str)
        except Exception as e:
            raise AirflowException(f"[Sales DQ] Failed to convert column {col} to {expected_type}: {str(e)}")

    # 2. Null checks for key columns
    essential_cols = ["period", "stateid", "sectorid"]
    for col in essential_cols:
        if df[col].isnull().any():
            raise AirflowException(f"[Sales DQ] Null values found in column: {col}")

    # 3. Uniqueness check (composite key: period + stateid + sectorid)
    if df.duplicated(subset=["period", "stateid", "sectorid"]).any():
        raise AirflowException("[Sales DQ] Duplicate rows found based on period+stateid+sectorid")

    # 4. Volume check
    row_count = len(df)
    if row_count < 100 or row_count > 10000:
        raise AirflowException(f"[Sales DQ] Volume check failed: {row_count} rows found")

    print("Pre-ingestion DQ for sales data passed.")

def load_eia_sales_data(**context):
    file_path = context['ti'].xcom_pull(key='sales_file_path')

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        print("No sales data found")
        return

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    create_table_sql = """
        CREATE TABLE IF NOT EXISTS eia_monthly_sales (
            period DATE,
            state_id TEXT,
            state_desc TEXT,
            sector_id TEXT,
            sector_name TEXT,
            customers BIGINT,
            price NUMERIC,
            revenue NUMERIC,
            sales NUMERIC,
            customers_units TEXT,
            price_units TEXT,
            revenue_units TEXT,
            sales_units TEXT
        );
    """
    cursor.execute(create_table_sql)

    insert_sql = """
        INSERT INTO eia_monthly_sales (
            period, state_id, state_desc, sector_id, sector_name,
            customers, price, revenue, sales,
            customers_units, price_units, revenue_units, sales_units
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows = [
        (
            f"{r['period']}-01", r["stateid"], r["stateDescription"],
            r["sectorid"], r["sectorName"], r["customers"], r["price"],
            r["revenue"], r["sales"], r["customers-units"],
            r["price-units"], r["revenue-units"], r["sales-units"]
        )
        for r in records
    ]

    cursor.executemany(insert_sql, rows)
    conn.commit()
    cursor.close()
    conn.close()

    print(f"Inserted {len(rows)} rows into eia_monthly_sales.")

def run_post_ingestion_dq(**context):
    from airflow.hooks.postgres_hook import PostgresHook

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    dq_checks = [
        {
            "check_name": "Check value is non-negative",
            "sql": "SELECT COUNT(*) FROM eia_fuel_type_data WHERE value < 0;",
            "expected_result": 0,
        },
        {
            "check_name": "Check no nulls in period",
            "sql": "SELECT COUNT(*) FROM eia_fuel_type_data WHERE period IS NULL;",
            "expected_result": 0,
        },
        # Add more checks here...
    ]

    report_lines = ["<table border='1'><tr><th>Check</th><th>Status</th><th>Actual</th><th>Expected</th></tr>"]
    for check in dq_checks:
        cursor.execute(check["sql"])
        actual_result = cursor.fetchone()[0]
        status = "✅ Passed" if actual_result == check["expected_result"] else "❌ Failed"

        report_lines.append(
            f"<tr><td>{check['check_name']}</td><td>{status}</td><td>{actual_result}</td><td>{check['expected_result']}</td></tr>"
        )

    report_lines.append("</table>")
    dq_report = "\n".join(report_lines)

    context['ti'].xcom_push(key='dq_report', value=dq_report)

    cursor.close()
    conn.close()



default_args = {
    "owner": "airflow"
    #"retries": 1,
    #"retry_delay": timedelta(minutes=5)
}

with DAG(
    dag_id="eia_daily_ingestion",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    max_active_runs=1,
    schedule_interval="0 19 * * *",  # Run daily at 7 PM
    catchup=True,
    tags=["eia", "postgres"],
) as dag:

    fetch_fuel_task = PythonOperator(
    task_id="fetch_eia_fuel_data",
    python_callable=fetch_eia_fuel_data,
    retries=3,
    retry_delay=timedelta(minutes=5),
    execution_timeout=timedelta(minutes=2),
    provide_context=True,
    on_failure_callback=failure_email_alert,
    )

    pre_dq_check_fuel = PythonOperator(
        task_id="run_pre_ingestion_dq_fuel",
        python_callable=run_pre_ingestion_dq_fuel,
        provide_context=True,
        on_failure_callback=failure_email_alert,
    )

    load_fuel_task = PythonOperator(
    task_id="load_eia_fuel_data",
    python_callable=load_eia_fuel_data,
    provide_context=True,
    on_failure_callback=failure_email_alert,
    )

    pre_dq_check_sales = PythonOperator(
    task_id="run_pre_ingestion_dq_sales",
    python_callable=run_pre_ingestion_dq_sales,
    provide_context=True,
    on_failure_callback=failure_email_alert,
    )

    fetch_sales_task = PythonOperator(
    task_id="fetch_eia_sales_data",
    python_callable=fetch_eia_sales_data,
    provide_context=True,
    on_failure_callback=failure_email_alert,
    )

    load_sales_task = PythonOperator(
    task_id="load_eia_sales_data",
    python_callable=load_eia_sales_data,
    provide_context=True,
    on_failure_callback=failure_email_alert,
    )

    post_dq_check = PythonOperator(
    task_id="post_ingestion_dq_check",
    python_callable=run_post_ingestion_dq,
    provide_context=True,
    on_failure_callback=failure_email_alert,
    )

    notify_success = PythonOperator(
    task_id="notify_success",
    python_callable=notify_success,
    provide_context=True,
    trigger_rule="all_success",
    )


    # DAG structure
    fetch_fuel_task >> pre_dq_check_fuel >> load_fuel_task
    fetch_sales_task >> pre_dq_check_sales >> load_sales_task

    [load_fuel_task, load_sales_task] >> post_dq_check >> notify_success
