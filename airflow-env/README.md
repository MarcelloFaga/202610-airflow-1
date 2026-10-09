# Airflow project

This folder contains a local Apache Airflow project based on Docker Compose.

## Project structure

- `dags/`: DAG files written in Python
- `logs/`: task and scheduler logs
- `plugins/`: custom Airflow plugins
- `config/`: optional Airflow configuration overrides
- `docker-compose.yaml`: local Airflow stack
- `.env`: local environment variables for Docker Compose

## Start the project

From this folder:

```bash
docker compose up airflow-init
docker compose up -d
```

Then open Airflow at http://localhost:8080

Default login:

- Username: airflow
- Password: airflow

## Stop the project

```bash
docker compose down
```

## Current DAG

The example DAG is in `dags/first-dag.py` and includes:

- a bash task to print the date
- Python tasks to print runtime information
- a templated bash task
