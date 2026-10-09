"""My third dag"""

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
    "owner": "Logan",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    "third-dag",
    default_args=default_args,
    description="Print a secret message",
    schedule="@daily",
    start_date=datetime(2026, 10, 7),
    catchup=True,
    tags=["what a bash"],
) as dag:
    print_message = BashOperator(
        task_id="print_message",
        bash_command='echo "Hello I''m Logan"',
    )

    dag.doc_md = __doc__