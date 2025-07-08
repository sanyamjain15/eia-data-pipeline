import argparse
from datetime import datetime, timedelta
import subprocess
import json

def get_existing_dag_runs(dag_id):
    """Fetch existing DAG run execution dates using the Airflow CLI inside Docker."""
    result = subprocess.run(
        ["docker", "exec", "airflow-docker-airflow-scheduler-1", "airflow", "dags", "list-runs", "-d", dag_id, "--output", "json"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode != 0:
        print(f"⚠️  Failed to list existing runs for DAG {dag_id}: {result.stderr}")
        return set()

    try:
        dag_runs = json.loads(result.stdout)
        return set(run["execution_date"] for run in dag_runs)
    except Exception as e:
        print(f"❌ Error parsing DAG run output: {e}")
        return set()

def trigger_dag_run(dag_id, exec_date):
    """Trigger a single DAG run if it doesn't already exist."""
    print(f"🚀 Attempting to trigger DAG '{dag_id}' for {exec_date}...")

    result = subprocess.run(
        ["docker", "exec", "airflow-docker-airflow-scheduler-1", "airflow", "dags", "trigger", dag_id, "--exec-date", exec_date],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if result.returncode == 0:
        print(f"✅ Triggered DAG run for {exec_date}")
    else:
        print(f"❌ Failed for {exec_date}:\n{result.stderr.strip()}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--dag-id", required=True)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    args = parser.parse_args()

    dag_id = args.dag_id
    start = datetime.strptime(args.start_date, "%Y-%m-%d")
    end = datetime.strptime(args.end_date, "%Y-%m-%d")

    existing_runs = get_existing_dag_runs(dag_id)
    existing_dates = {r.split("T")[0] for r in existing_runs}  # e.g., "2023-01-01"

    print(f"\n📅 Checking from {start.date()} to {end.date()}...")
    for n in range((end - start).days + 1):
        exec_date = start + timedelta(days=n)
        exec_date_str = exec_date.strftime("%Y-%m-%d")

        if exec_date_str in existing_dates:
            print(f"⚠️  Skipping {exec_date_str} — already exists.")
        else:
            trigger_dag_run(dag_id, exec_date.strftime("%Y-%m-%dT00:00:00"))

if __name__ == "__main__":
    main()
