import os
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.sensors.filesystem import FileSensor  # pyright: ignore[reportMissingImports]


def log_pipeline_start(**context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    context["ti"].log.info("Pipeline started | dag_id=%s | run_id=%s", dag_id, run_id)


def log_pipeline_finish(**context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    context["ti"].log.info("Pipeline finished | dag_id=%s | run_id=%s", dag_id, run_id)


def log_task_failure(context) -> None:
    ti = context["task_instance"]
    exception = context.get("exception")
    ti.log.error(
        "Spark task failed | dag_id=%s | task_id=%s | run_id=%s | try_number=%s | exception=%s",
        ti.dag_id,
        ti.task_id,
        ti.run_id,
        ti.try_number,
        exception,
    )


PROJECT_ROOT = os.environ.get("PROJECT_ROOT", "/opt/airflow")
SPARK_JOB_PATH = os.environ.get(
    "SPARK_JOB_PATH",
    os.path.join(PROJECT_ROOT, "spark_jobs", "medallion_pipeline.py"),
)
QUALITY_CHECK_PATH = os.environ.get(
    "QUALITY_CHECK_PATH",
    os.path.join(PROJECT_ROOT, "spark_jobs", "quality_check.py"),
)
INPUT_FILE_GLOB = os.environ.get("INPUT_FILE_GLOB", "/data/input/*")
FS_CONN_ID = os.environ.get("FS_CONN_ID", "fs_default")

SPARK_SUBMIT_CMD = [
    "/opt/spark/bin/spark-submit",
    "--packages", "io.delta:delta-spark_2.12:3.2.0",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]


def _submit_to_spark(script_path: str, context: dict) -> None:
    """Run a PySpark script inside the running spark container via the Docker SDK."""
    import docker

    ti_log = context["ti"].log
    ti_log.info("[PIPELINE] Submitting script to Spark container | script=%s", script_path)

    client = docker.from_env()
    try:
        container = client.containers.get("spark")
    except docker.errors.NotFound as exc:
        raise RuntimeError(
            "Spark container 'spark' is not running. "
            "Start it with: docker compose up -d spark"
        ) from exc

    exec_id = client.api.exec_create(
        container.id,
        SPARK_SUBMIT_CMD + [script_path],
    )["Id"]

    for chunk in client.api.exec_start(exec_id, stream=True):
        for line in chunk.decode("utf-8", errors="replace").splitlines():
            if line.strip():
                ti_log.info(line)

    exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    ti_log.info("[PIPELINE] Script finished | exit_code=%s | script=%s", exit_code, script_path)

    if exit_code != 0:
        raise RuntimeError(f"spark-submit exited with code {exit_code} for script: {script_path}")


def _run_spark_job(**context) -> None:
    _submit_to_spark(SPARK_JOB_PATH, context)


def _run_quality_check(**context) -> None:
    _submit_to_spark(QUALITY_CHECK_PATH, context)


DEFAULT_ARGS = {
    "owner": "data-eng",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_pyspark_pipeline",
    description="Runs the PySpark Delta pipeline daily at 5AM",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="0 5 * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "delta", "scheduled"],
) as daily_dag:
    daily_start_log = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
    )

    daily_run_spark = PythonOperator(
        task_id="run_process_data",
        python_callable=_run_spark_job,
        on_failure_callback=log_task_failure,
    )

    daily_quality_check = PythonOperator(
        task_id="run_quality_check",
        python_callable=_run_quality_check,
        on_failure_callback=log_task_failure,
    )

    daily_finish_log = PythonOperator(
        task_id="log_pipeline_finish",
        python_callable=log_pipeline_finish,
        trigger_rule="all_done",
    )

    daily_start_log >> daily_run_spark >> daily_quality_check >> daily_finish_log


with DAG(
    dag_id="event_driven_pyspark_pipeline",
    description="Monitors data/input and runs PySpark pipeline when a new file appears",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="* * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "delta", "event-driven", "sensor"],
) as event_dag:
    detect_new_input_file = FileSensor(
        task_id="detect_new_input_file",
        fs_conn_id=FS_CONN_ID,
        filepath=INPUT_FILE_GLOB,
        poke_interval=1,
        timeout=3600,
        mode="poke",
    )

    event_start_log = PythonOperator(
        task_id="log_pipeline_start",
        python_callable=log_pipeline_start,
    )

    event_run_spark = PythonOperator(
        task_id="run_process_data",
        python_callable=_run_spark_job,
        on_failure_callback=log_task_failure,
    )

    event_quality_check = PythonOperator(
        task_id="run_quality_check",
        python_callable=_run_quality_check,
        on_failure_callback=log_task_failure,
    )

    event_finish_log = PythonOperator(
        task_id="log_pipeline_finish",
        python_callable=log_pipeline_finish,
        trigger_rule="all_done",
    )

    detect_new_input_file >> event_start_log >> event_run_spark >> event_quality_check >> event_finish_log
