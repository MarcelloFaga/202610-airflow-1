"""Download flowers dataset, compute volume, then branch on a random condition."""

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator

DATA_URL = "https://raw.githubusercontent.com/CourseMaterial/DataWrangling/main/flowerdataset.csv"
INPUT_PATH = Path("/opt/airflow/dags/input/flowerdataset.csv")
OUTPUT_PATH = Path("/opt/airflow/dags/output/flowerdataset_with_volume.csv")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 20),
    "email": ["email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 4,
    "retry_delay": timedelta(minutes=1),
}


def add_volume_column() -> None:
    """Read the downloaded CSV, compute volume, and write the enriched file."""

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

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


def choose_message_branch() -> str:
    """Pick one branch at random to keep the example simple."""

    return random.choice(["belle_journee_pour_ramasser_des_fleurs", "malheureusement_il_faut_travailler"])


with DAG(
    "second-dag-random",
    default_args=default_args,
    description="Download a flower dataset and add a volume column",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 3, 20),
    catchup=False,
    tags=["flowers for volume"],
) as dag:
    download_data = BashOperator(
        task_id="download_data",
        bash_command=f"mkdir -p {INPUT_PATH.parent} && curl -sSL {DATA_URL} -o {INPUT_PATH}",
    )

    process_data = PythonOperator(
        task_id="process_data",
        python_callable=add_volume_column,
    )

    choose_branch = BranchPythonOperator(
        task_id="choose_branch",
        python_callable=choose_message_branch,
    )

    belle_journee_pour_ramasser_des_fleurs = BashOperator(
        task_id="belle_journee_pour_ramasser_des_fleurs",
        bash_command='echo "Belle journée pour ramasser des fleurs"',
    )

    malheureusement_il_faut_travailler = BashOperator(
        task_id="malheureusement_il_faut_travailler",
        bash_command='echo "Malheureusement il faut travailler"',
    )

    download_data >> process_data >> choose_branch
    choose_branch >> [
        belle_journee_pour_ramasser_des_fleurs,
        malheureusement_il_faut_travailler,
    ]

    dag.doc_md = __doc__