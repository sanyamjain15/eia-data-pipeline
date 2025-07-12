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
import logging


# Config
EIA_API_KEY = os.getenv("EIA_API_KEY")
EMAIL_ID = os.getenv("SMTP_USER")
DATA_DIR = "/opt/airflow/data"

def notify_failure(context):
    """
    Sends an email alert when any task fails in the DAG.
    Includes task name, dag id, error details, and execution date.
    """

    dag_id = context.get("dag").dag_id
    task_id = context.get("task_instance").task_id
    execution_date = context.get("ds")
    exception = context.get("exception")

    subject = f"[ALERT] Airflow Task Failed: {dag_id}.{task_id}"
    body = f"""
    <h2>❌ Airflow Task Failure Alert</h2>
    <p><b>DAG:</b> {dag_id}<br>
       <b>Task:</b> {task_id}<br>
       <b>Execution Date:</b> {execution_date}</p>
    <h3 style="color:red;">Error Details</h3>
    <pre>{str(exception)[:1000]}</pre>
    <p>Check the Airflow UI for full logs.</p>
    """

    send_email(
        to=[str(EMAIL_ID)],  # Update to team distro if needed
        subject=subject,
        html_content=body
    )

def notify_success(**context):
    """
    Sends a success email after the DAG completes.
    Includes post-ingestion DQ results and row ingestion summary.
    """

    dag_run = context.get('dag_run')
    execution_date = context.get('ds')
    dag_id = context.get('dag').dag_id

    # Pull the HTML DQ report from XCom
    dq_report = context['ti'].xcom_pull(task_ids='post_ingestion_dq_check', key='dq_report') or "<p>No report generated.</p>"

    subject = f"[SUCCESS] DAG '{dag_id}' - {execution_date}"
    body = f"""
    <h2>✅ Daily EIA Data Ingestion Successful</h2>
    <p>DAG ID: <b>{dag_id}</b><br>
    Execution Date: <b>{execution_date}</b></p>
    {dq_report}
    <p style="color:gray; font-size:0.9em;">This is an automated report from Airflow.</p>
    """

    send_email(
        to=[str(EMAIL_ID)],  # Replace or generalize as needed
        subject=subject,
        html_content=body
    )

def fetch_eia_fuel_data(**context):
    """
    Fetches daily fuel-type generation data from the EIA API and stores it as JSON.
    Pushes file path and target date to XCom.
    """
    logging.info("==== [START] Fetch EIA Fuel Data ====")

    execution_date = context['execution_date']
    target_date = (execution_date - timedelta(days=1)).strftime("%Y-%m-%d")

    url = (
        f"https://api.eia.gov/v2/electricity/rto/daily-fuel-type-data/data/"
        f"?api_key={EIA_API_KEY}&frequency=daily&data[0]=value"
        f"&start={target_date}&end={target_date}"
        f"&sort[0][column]=period&sort[0][direction]=desc"
        f"&offset=0&length=5000"
    )

    logging.info(f"[Fetch] Requesting EIA data for date: {target_date}")
    logging.info(f"[Fetch] URL: {url}")

    response = requests.get(url)
    logging.info(f"[Fetch] HTTP Status: {response.status_code}")

    if response.status_code != 200:
        logging.error(f"[Fetch] Failed API response: {response.text}")
        raise Exception(f"Failed to fetch data: {response.status_code}")

    data = response.json()

    # Save to file
    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, f"eia_data_{target_date}.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logging.info(f"[Fetch] Saved raw JSON to: {file_path}")

    # Push to XCom
    context['ti'].xcom_push(key='file_path', value=file_path)
    context['ti'].xcom_push(key='target_date', value=target_date)

    logging.info(f"[Fetch] XCom pushed: file_path={file_path}, target_date={target_date}")
    logging.info("==== [END] Fetch EIA Fuel Data ====")

def run_pre_ingestion_dq_fuel(**context):
    """
    Runs pre-ingestion data quality checks for EIA daily fuel data.
    Validates schema, data types, nulls, uniqueness, and row count.
    """
    logging.info("==== [START] Pre-Ingestion DQ for EIA Fuel Data ====")

    file_path = context['ti'].xcom_pull(key='file_path')
    if not file_path or not os.path.exists(file_path):
        logging.error("[DQ-Fuel] File not found at path: %s", file_path)
        raise AirflowException("Pre-ingestion DQ failed: file not found.")

    logging.info("[DQ-Fuel] File path confirmed: %s", file_path)

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        logging.error("[DQ-Fuel] No records found in API response.")
        raise AirflowException("Pre-ingestion DQ failed: no data in file.")

    df = pd.DataFrame(records)
    logging.info("[DQ-Fuel] Loaded %d records into DataFrame", len(df))

    # Schema and data types to validate
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

    # Step 1: Validate schema and types
    for col, expected_type in expected_schema.items():
        if col not in df.columns:
            logging.error("[DQ-Fuel] Missing column in schema: %s", col)
            raise AirflowException(f"Missing column in schema: {col}")
        try:
            if expected_type in [int, float, (int, float)]:
                df[col] = pd.to_numeric(df[col], errors='raise')
            else:
                df[col] = df[col].astype(str)
        except Exception as e:
            logging.error("[DQ-Fuel] Type conversion failed for column %s: %s", col, str(e))
            raise AirflowException(f"Failed to convert column {col} to {expected_type}: {str(e)}")
    logging.info("[DQ-Fuel] Schema and data types validated")

    # Step 2: Null checks
    essential_cols = ["period", "respondent", "fueltype", "value"]
    for col in essential_cols:
        if df[col].isnull().any():
            null_count = df[col].isnull().sum()
            logging.error("[DQ-Fuel] Nulls found in column %s: %d", col, null_count)
            raise AirflowException(f"Null values found in {col}")
    logging.info("[DQ-Fuel] Null check passed for essential columns")

    # Step 3: Duplicate check on PK
    if df.duplicated(subset=["period", "respondent", "fueltype", "timezone"]).any():
        logging.error("[DQ-Fuel] Duplicate rows found based on PK subset")
        raise AirflowException("Duplicate rows found based on PK subset")
    logging.info("[DQ-Fuel] Duplicate check passed")

    # Step 4: Row volume check
    row_count = len(df)
    if row_count < 100 or row_count > 10000:
        logging.error("[DQ-Fuel] Volume check failed: %d rows", row_count)
        raise AirflowException(f"Volume check failed: {row_count} rows found")
    logging.info("[DQ-Fuel] Row volume check passed (%d rows)", row_count)

    logging.info("==== [END] Pre-Ingestion DQ for EIA Fuel Data ====")

def load_eia_fuel_data(**context):
    """
    Loads daily fuel-type generation data from a local JSON file into a PostgreSQL table.
    - Reads metadata from XCom: `file_path` and `target_date`
    - Creates table `eia_fuel_type_data` if it does not exist
    - Inserts records with deduplication using ON CONFLICT
    """
    logging.info("==== [START] Load EIA Fuel Data ====")

    # Pull metadata from XCom
    file_path = context['ti'].xcom_pull(key='file_path')
    target_date = context['ti'].xcom_pull(key='target_date')
    logging.info(f"[Load] Retrieved file_path: {file_path}")
    logging.info(f"[Load] Retrieved target_date: {target_date}")

    if not os.path.exists(file_path):
        logging.error(f"[Load] File does not exist: {file_path}")
        raise FileNotFoundError(f"Data file not found: {file_path}")

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        logging.warning(f"[Load] No data records found for {target_date}. Skipping insertion.")
        return

    logging.info(f"[Load] Number of records to insert: {len(records)}")

    # Connect to Postgres
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    try:
        # Step 1: Create table
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
        logging.info("[Load] Table 'eia_fuel_type_data' ensured.")

        # Step 2: Insert records
        insert_sql = """
            INSERT INTO eia_fuel_type_data (
                period, respondent, respondent_name, fuel_type, type_name, timezone, value, value_units
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (period, respondent, fuel_type, timezone)
            DO NOTHING;
        """

        rows = [
            (
                r["period"], r["respondent"], r["respondent-name"],
                r["fueltype"], r["type-name"], r["timezone"],
                r["value"], r["value-units"]
            )
            for r in records
        ]

        cursor.executemany(insert_sql, rows)
        conn.commit()
        logging.info(f"[Load] Inserted {len(rows)} rows into 'eia_fuel_type_data'.")

        # Push inserted row count to XCom for reporting
        context['ti'].xcom_push(key='eia_fuel_type_data_inserted_count', value=len(rows))

    except Exception as e:
        logging.error(f"[Load] Error inserting data: {str(e)}")
        conn.rollback()
        raise

    finally:
        cursor.close()
        conn.close()
        logging.info("[Load] Database connection closed.")

    logging.info("==== [END] Load EIA Fuel Data ====")

def fetch_eia_sales_data(**context):
    """
    Fetches monthly retail electricity sales data from the EIA API.
    Saves the response as a JSON file and pushes its path to XCom.
    """
    logging.info("==== [START] Fetch EIA Sales Data ====")

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
        "&start=2025-01"
    )

    logging.info("[Sales Fetch] Requesting data from EIA API")
    logging.info(f"[Sales Fetch] URL: {url}")

    try:
        response = requests.get(url)
        logging.info(f"[Sales Fetch] HTTP Status Code: {response.status_code}")
    except Exception as e:
        logging.error(f"[Sales Fetch] Request failed: {str(e)}")
        raise

    if response.status_code != 200:
        logging.error(f"[Sales Fetch] API error: {response.text}")
        raise Exception(f"Failed to fetch monthly sales data: {response.status_code}, {response.text}")

    data = response.json()

    os.makedirs(DATA_DIR, exist_ok=True)
    file_path = os.path.join(DATA_DIR, "eia_sales_data.json")
    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    logging.info(f"[Sales Fetch] Saved sales data to: {file_path}")

    context['ti'].xcom_push(key='sales_file_path', value=file_path)
    logging.info(f"[Sales Fetch] XCom pushed: sales_file_path={file_path}")

    logging.info("==== [END] Fetch EIA Sales Data ====")

def run_pre_ingestion_dq_sales(**context):
    """
    Runs pre-ingestion data quality checks for EIA monthly sales data.
    Validates schema, data types, nulls, uniqueness, and row count.
    """
    logging.info("==== [START] Pre-Ingestion DQ for EIA Sales Data ====")

    file_path = context['ti'].xcom_pull(key='sales_file_path')
    if not file_path or not os.path.exists(file_path):
        logging.error("[DQ-Sales] File not found at path: %s", file_path)
        raise AirflowException("Pre-ingestion DQ failed: sales file not found.")

    logging.info("[DQ-Sales] File path confirmed: %s", file_path)

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        logging.error("[DQ-Sales] No records found in sales file.")
        raise AirflowException("Pre-ingestion DQ failed: no sales data in file.")

    df = pd.DataFrame(records)
    logging.info("[DQ-Sales] Loaded %d records into DataFrame", len(df))

    # Step 1: Schema and data type checks
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
            logging.error("[DQ-Sales] Missing column in schema: %s", col)
            raise AirflowException(f"[Sales DQ] Missing column: {col}")
        try:
            if expected_type in [int, float, (int, float)]:
                df[col] = pd.to_numeric(df[col], errors='raise')
            else:
                df[col] = df[col].astype(str)
        except Exception as e:
            logging.error("[DQ-Sales] Failed to convert column %s: %s", col, str(e))
            raise AirflowException(f"[Sales DQ] Failed to convert column {col} to {expected_type}: {str(e)}")
    logging.info("[DQ-Sales] Schema and data types validated")

    # Step 2: Null checks for essential columns
    essential_cols = ["period", "stateid", "sectorid"]
    for col in essential_cols:
        if df[col].isnull().any():
            null_count = df[col].isnull().sum()
            logging.error("[DQ-Sales] Nulls found in column %s: %d", col, null_count)
            raise AirflowException(f"[Sales DQ] Null values found in column: {col}")
    logging.info("[DQ-Sales] Null check passed for essential columns")

    # Step 3: Uniqueness check
    if df.duplicated(subset=["period", "stateid", "sectorid"]).any():
        logging.error("[DQ-Sales] Duplicate rows found on period + stateid + sectorid")
        raise AirflowException("[Sales DQ] Duplicate rows found based on period+stateid+sectorid")
    logging.info("[DQ-Sales] Duplicate check passed")

    # Step 4: Row volume check
    row_count = len(df)
    if row_count < 100 or row_count > 10000:
        logging.error("[DQ-Sales] Volume check failed: %d rows", row_count)
        raise AirflowException(f"[Sales DQ] Volume check failed: {row_count} rows found")
    logging.info("[DQ-Sales] Row volume check passed (%d rows)", row_count)

    logging.info("==== [END] Pre-Ingestion DQ for EIA Sales Data ====")

def load_eia_sales_data(**context):
    """
    Loads EIA monthly sales data from JSON file and inserts it into Postgres.
    Assumes data has already been pre-validated and saved to disk.
    """
    logging.info("==== [START] Load EIA Sales Data into Postgres ====")

    file_path = context['ti'].xcom_pull(key='sales_file_path')
    logging.info(f"[Sales Load] Retrieved file path from XCom: {file_path}")

    if not file_path or not os.path.exists(file_path):
        logging.error("[Sales Load] JSON file not found.")
        raise FileNotFoundError(f"File not found: {file_path}")

    with open(file_path, "r") as f:
        data = json.load(f)

    records = data.get("response", {}).get("data", [])
    if not records:
        logging.warning("[Sales Load] No records found in the sales dataset.")
        return

    logging.info(f"[Sales Load] {len(records)} records loaded from file")

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()
    logging.info("[Sales Load] Connected to Postgres")

    # Step 1: Create table if not exists
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
    conn.commit()
    logging.info("[Sales Load] Ensured table eia_monthly_sales exists")

    # Step 2: Prepare insert
    insert_sql = """
        INSERT INTO eia_monthly_sales (
            period, state_id, state_desc, sector_id, sector_name,
            customers, price, revenue, sales,
            customers_units, price_units, revenue_units, sales_units
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows = [
        (
            f"{r['period']}-01",  # Append day to make a full date
            r["stateid"], r["stateDescription"],
            r["sectorid"], r["sectorName"],
            r["customers"], r["price"], r["revenue"], r["sales"],
            r["customers-units"], r["price-units"], r["revenue-units"], r["sales-units"]
        )
        for r in records
    ]

    cursor.executemany(insert_sql, rows)
    conn.commit()

    logging.info(f"[Sales Load] Inserted {len(rows)} rows into eia_monthly_sales")

    # Push inserted row count to XCom for reporting
    context['ti'].xcom_push(key='eia_monthly_sales_inserted_count', value=len(rows))

    cursor.close()
    conn.close()
    logging.info("[Sales Load] Database connection closed")

    logging.info("==== [END] Load EIA Sales Data into Postgres ====")

def run_post_ingestion_dq(**context):

    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    # Step 1: Post-ingestion DQ Checks
    dq_checks = [
        {
            "table": "eia_fuel_type_data",
            "check_name": "Value should be non-negative",
            "sql": "SELECT COUNT(*) FROM eia_fuel_type_data WHERE value < 0;",
            "expected_result": 0,
        },
        {
            "table": "eia_fuel_type_data",
            "check_name": "No NULLs in period",
            "sql": "SELECT COUNT(*) FROM eia_fuel_type_data WHERE period IS NULL;",
            "expected_result": 0,
        },
        {
            "table": "eia_monthly_sales",
            "check_name": "No NULLs in state_id",
            "sql": "SELECT COUNT(*) FROM eia_monthly_sales WHERE state_id IS NULL;",
            "expected_result": 0,
        },
    ]

    dq_results = []
    report_lines = ["<h3>Post-Ingestion Data Quality Checks</h3>"]
    report_lines.append("<table border='1'><tr><th>Table</th><th>Check</th><th>Status</th><th>Actual</th><th>Expected</th></tr>")

    for check in dq_checks:
        cursor.execute(check["sql"])
        actual = cursor.fetchone()[0]
        status = "✅ Passed" if actual == check["expected_result"] else "❌ Failed"

        dq_results.append((check["table"], check["check_name"], status, actual, check["expected_result"]))
        report_lines.append(
            f"<tr><td>{check['table']}</td><td>{check['check_name']}</td><td>{status}</td><td>{actual}</td><td>{check['expected_result']}</td></tr>"
        )

    report_lines.append("</table>")

    # Step 2: Row Ingestion Summary
    row_tables = ["eia_fuel_type_data", "eia_monthly_sales"]
    ingestion_lines = ["<h3>Row Ingestion Summary</h3>"]
    ingestion_lines.append("<table border='1'><tr><th>Table</th><th>Previous</th><th>Inserted</th><th>Current</th></tr>")

    for table in row_tables:
        # Use XCom-pushed value from insertion tasks
        inserted = context['ti'].xcom_pull(key=f'{table}_inserted_count') or 0
        cursor.execute(f"SELECT COUNT(*) FROM {table};")
        current = cursor.fetchone()[0]
        previous = current - inserted
        ingestion_lines.append(f"<tr><td>{table}</td><td>{previous}</td><td>{inserted}</td><td>{current}</td></tr>")

    ingestion_lines.append("</table>")

    dq_report = "\n".join(report_lines + ingestion_lines)
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
    on_failure_callback=notify_failure,
    )

    pre_dq_check_fuel = PythonOperator(
        task_id="run_pre_ingestion_dq_fuel",
        python_callable=run_pre_ingestion_dq_fuel,
        provide_context=True,
        on_failure_callback=notify_failure,
    )

    load_fuel_task = PythonOperator(
    task_id="load_eia_fuel_data",
    python_callable=load_eia_fuel_data,
    provide_context=True,
    on_failure_callback=notify_failure,
    )

    pre_dq_check_sales = PythonOperator(
    task_id="run_pre_ingestion_dq_sales",
    python_callable=run_pre_ingestion_dq_sales,
    provide_context=True,
    on_failure_callback=notify_failure,
    )

    fetch_sales_task = PythonOperator(
    task_id="fetch_eia_sales_data",
    python_callable=fetch_eia_sales_data,
    provide_context=True,
    on_failure_callback=notify_failure,
    )

    load_sales_task = PythonOperator(
    task_id="load_eia_sales_data",
    python_callable=load_eia_sales_data,
    provide_context=True,
    on_failure_callback=notify_failure,
    )

    post_dq_check = PythonOperator(
    task_id="post_ingestion_dq_check",
    python_callable=run_post_ingestion_dq,
    provide_context=True,
    on_failure_callback=notify_failure,
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
