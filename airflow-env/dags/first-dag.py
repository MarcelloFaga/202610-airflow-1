"""Download a flower dataset and add a volume column."""

import csv
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

DATA_URL = "https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv"
INPUT_PATH = Path("/tmp/flowerdataset.csv")
OUTPUT_PATH = Path("/tmp/flowerdataset_with_volume.csv")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=1),
}


def add_volume_column() -> None:
    """Read the downloaded CSV, compute volume, and write the enriched file."""

    with INPUT_PATH.open(newline="", encoding="utf-8") as source_file, OUTPUT_PATH.open(
        "w", newline="", encoding="utf-8"
    ) as target_file:
        reader = csv.DictReader(source_file)
        fieldnames = list(reader.fieldnames or [])

        if "volume" not in fieldnames:
            fieldnames.append("volume")

        writer = csv.DictWriter(target_file, fieldnames=fieldnames)
        writer.writeheader()

        row_count = 0
        for row in reader:
            sepal_length = float(row["sepal_length"])
            sepal_width = float(row["sepal_width"])
            row["volume"] = sepal_length * sepal_width
            writer.writerow(row)
            row_count += 1

    print(f"Wrote {row_count} rows to {OUTPUT_PATH}")


def print_result_path() -> None:
    """Log the location of the processed dataset."""

    print(f"Processed file available at: {OUTPUT_PATH}")


with DAG(
    "first-dag",
    default_args=default_args,
    description="Download a flower dataset and add a volume column",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 3, 20),
    catchup=False,
    tags=["example", "flowers", "python"],
) as dag:
    download_data = BashOperator(
        task_id="download_data",
        bash_command=f"curl -sSL {DATA_URL} -o {INPUT_PATH}",
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=add_volume_column,
    )

    show_result = PythonOperator(
        task_id="show_result",
        python_callable=print_result_path,
    )

    download_data >> process_data >> show_result

    dag.doc_md = __doc__
