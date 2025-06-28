from airflow import DAG
from airflow.operators.email import EmailOperator
from datetime import datetime

with DAG("test_email_dag", start_date=datetime(2025, 1, 1), schedule_interval=None, catchup=False) as dag:
    send_email = EmailOperator(
        task_id="send_test_email",
        to="sam.jain1511@gmail.com",
        subject="Airflow Email Test",
        html_content="This is a test email from Airflow",
    )
