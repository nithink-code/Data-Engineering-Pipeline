import os
import json
import glob
from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.operators.python import ShortCircuitOperator  # pyright: ignore[reportMissingImports]


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
INPUT_FILE_GLOB = os.environ.get("INPUT_FILE_GLOB", "/data/input/*")
INPUT_SNAPSHOT_PATH = os.environ.get("INPUT_SNAPSHOT_PATH", "/data/.input_snapshot.json")


def _collect_input_snapshot() -> dict:
    files = []
    for path in sorted(glob.glob(INPUT_FILE_GLOB)):
        if not os.path.isfile(path):
            continue
        stat_info = os.stat(path)
        files.append(
            {
                "path": path,
                "size": stat_info.st_size,
                "mtime_ns": stat_info.st_mtime_ns,
            }
        )

    return {
        "glob": INPUT_FILE_GLOB,
        "files": files,
    }


def _should_run_for_input_change(**context) -> bool:
    """Return True only when input files changed since the last event DAG run.

    This keeps automation responsive while avoiding expensive Spark submits when
    there is no new/updated/deleted file in data/input.
    """
    ti_log = context["ti"].log
    current_snapshot = _collect_input_snapshot()

    previous_snapshot = None
    if os.path.exists(INPUT_SNAPSHOT_PATH):
        try:
            with open(INPUT_SNAPSHOT_PATH, "r", encoding="utf-8") as snapshot_file:
                previous_snapshot = json.load(snapshot_file)
        except (OSError, json.JSONDecodeError) as exc:
            ti_log.warning("Could not read previous snapshot, forcing run | error=%s", exc)

    changed = current_snapshot != previous_snapshot
    if changed:
        os.makedirs(os.path.dirname(INPUT_SNAPSHOT_PATH), exist_ok=True)
        with open(INPUT_SNAPSHOT_PATH, "w", encoding="utf-8") as snapshot_file:
            json.dump(current_snapshot, snapshot_file, indent=2)
        ti_log.info("[PIPELINE] Input change detected | files=%s", len(current_snapshot["files"]))
    else:
        ti_log.info("[PIPELINE] No input change detected; skipping Spark run")

    return changed

def _run_spark_job(**context) -> None:
    """Execute spark-submit on the running spark container via the Docker SDK.

    Uses the Python docker SDK instead of the Docker CLI so that API version
    negotiation is handled automatically, avoiding the 'client too old' error
    that occurs when the CLI inside the Airflow image is behind the host daemon.
    """
    import docker  # imported lazily so the DAG parses even if SDK is mid-install

    ti_log = context["ti"].log
    ti_log.info("[PIPELINE] Spark job starting | job=%s", SPARK_JOB_PATH)

    client = docker.from_env()
    try:
        container = client.containers.get("spark")
    except docker.errors.NotFound as exc:
        raise RuntimeError(
            "Spark container 'spark' is not running. "
            "Start it with: docker compose up -d spark"
        ) from exc

    # exec_create / exec_start lets us stream output AND retrieve the exit code.
    # --packages downloads the Delta JAR before SparkContext init so that
    # DeltaCatalog and DeltaSparkSessionExtension are available at startup.
    # --conf spark.jars.ivy=/tmp/.ivy2 uses a writable cache dir in the container.
    exec_id = client.api.exec_create(
        container.id,
        [
            "/opt/spark/bin/spark-submit",
            "--packages", "io.delta:delta-spark_2.12:3.2.0",
            "--conf", "spark.jars.ivy=/tmp/.ivy2",
            "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
            "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
            SPARK_JOB_PATH,
        ],
    )["Id"]

    for chunk in client.api.exec_start(exec_id, stream=True):
        for line in chunk.decode("utf-8", errors="replace").splitlines():
            if line.strip():
                ti_log.info(line)

    exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    ti_log.info("[PIPELINE] Spark job finished | exit_code=%s", exit_code)

    if exit_code != 0:
        raise RuntimeError(f"spark-submit exited with code {exit_code}")


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

    daily_finish_log = PythonOperator(
        task_id="log_pipeline_finish",
        python_callable=log_pipeline_finish,
        trigger_rule="all_done",
    )

    daily_start_log >> daily_run_spark >> daily_finish_log


with DAG(
    dag_id="event_driven_pyspark_pipeline",
    description="Runs PySpark pipeline only when data/input changes",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="* * * * *",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "delta", "event-driven", "change-detection"],
) as event_dag:
    detect_input_change = ShortCircuitOperator(
        task_id="detect_input_change",
        python_callable=_should_run_for_input_change,
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

    event_finish_log = PythonOperator(
        task_id="log_pipeline_finish",
        python_callable=log_pipeline_finish,
        trigger_rule="all_done",
    )

    detect_input_change >> event_start_log >> event_run_spark >> event_finish_log
