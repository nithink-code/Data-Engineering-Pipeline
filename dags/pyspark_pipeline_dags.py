"""
pyspark_pipeline_dags.py
========================
Two DAGs:
  1. daily_pyspark_pipeline       — scheduled 05:00 UTC daily
  2. event_driven_pyspark_pipeline — triggers when a new file lands in data/input/

Both DAGs:
  • Write a pipeline_status.json on START / SUCCESS / ERROR so the
    Streamlit dashboard shows a live status banner.
  • Send an email via Resend on SUCCESS and immediately on ERROR
    (before retries, so the owner knows right away).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator    # pyright: ignore[reportMissingImports]
from airflow.sensors.filesystem import FileSensor      # pyright: ignore[reportMissingImports]

# ── Resolve project root and put it on the path ──────────────────────────────
_DAGS_DIR    = Path(__file__).resolve().parent        # .../dags/
_PROJECT_ROOT = _DAGS_DIR.parent                      # .../Data-Engineering-Pipeline/
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# ── Import shared utilities (graceful fallback if not installed) ──────────────
try:
    from utils.notifications import (
        trigger_error_email,
        trigger_success_email,
        send_pipeline_notification,   # kept for generic/test use
    )
except ImportError:
    def trigger_error_email(*args, **kwargs):     # type: ignore[misc]
        pass
    def trigger_success_email(*args, **kwargs):   # type: ignore[misc]
        pass
    def send_pipeline_notification(*args, **kwargs):  # type: ignore[misc]
        pass

try:
    from utils.pipeline_status import write_running, write_success, write_error
except ImportError:
    def write_running(*args, **kwargs):  # type: ignore[misc]
        pass
    def write_success(*args, **kwargs):  # type: ignore[misc]
        pass
    def write_error(*args, **kwargs):  # type: ignore[misc]
        pass

PROJECT_ROOT     = os.environ.get("PROJECT_ROOT", str(_PROJECT_ROOT))
SPARK_JOB_PATH   = os.environ.get(
    "SPARK_JOB_PATH",
    os.path.join(PROJECT_ROOT, "spark_jobs", "medallion_pipeline.py"),
)
INPUT_FILE_GLOB  = os.environ.get("INPUT_FILE_GLOB", "/data/input/*")
FS_CONN_ID       = os.environ.get("FS_CONN_ID", "fs_default")

SPARK_SUBMIT_CMD = [
    "/opt/spark/bin/spark-submit",
    "--packages", "io.delta:delta-spark_2.12:3.2.0",
    "--conf", "spark.jars.ivy=/tmp/.ivy2",
    "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
    "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
]


# ── Task callables ─────────────────────────────────────────────────────────────
def log_pipeline_start(**context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ti     = context["ti"]

    msg = f"Pipeline started | dag={dag_id} | run={run_id}"
    ti.log.info(msg)
    write_running(dag_id, ti.task_id, run_id)


def log_pipeline_finish(**context) -> None:
    dag_id = context["dag"].dag_id
    run_id = context["run_id"]
    ti     = context["ti"]

    ti.log.info("Pipeline finished successfully | dag=%s | run=%s", dag_id, run_id)

    # ── 1. Update shared status file (dashboard reads this) ───────────────────
    write_success(dag_id, run_id)

    # ── 2. Compute wall-clock duration ────────────────────────────────────────
    start_date = getattr(ti, "start_date", None)
    try:
        from datetime import timezone
        duration = (
            datetime.now(timezone.utc) - start_date
        ).total_seconds() if start_date else 0.0
    except Exception:
        duration = 0.0

    # ── 3. Count tasks that ran in this DAG run ────────────────────────────────
    task_count = 0
    try:
        dag_run    = context.get("dag_run")
        task_count = len(dag_run.get_task_instances()) if dag_run else 0
    except Exception:
        pass

    # ── 4. Send SUCCESS email ──────────────────────────────────────────────────
    try:
        trigger_success_email(
            dag_id           = dag_id,
            run_id           = run_id,
            duration_seconds = duration,
            task_count       = task_count,
        )
    except Exception as exc:  # noqa: BLE001
        ti.log.warning("Success email failed: %s", exc)


def _read_task_logs(ti) -> str:
    """Read the tail of the log file and extract 'Smart Highlights' (ERROR/Exception lines)."""
    try:
        from airflow.configuration import conf
        base_log_folder = conf.get("logging", "BASE_LOG_FOLDER")
        
        run_id = ti.run_id.replace(":", "_").replace("+", "_")
        log_path = Path(base_log_folder) / ti.dag_id / ti.task_id / run_id / f"{ti.try_number}.log"
        
        if not log_path.exists():
            exec_date = ti.execution_date.isoformat().replace(":", "_")
            log_path = Path(base_log_folder) / ti.dag_id / ti.task_id / exec_date / f"{ti.try_number}.log"

        if log_path.exists():
            with open(log_path, "r", encoding="utf-8", errors="replace") as f:
                all_lines = f.readlines()
                
                # ── 1. Extract Highlights (Scan last 500 lines for keywords) ────
                keywords = ["ERROR", "CRITICAL", "EXCEPTION", "SPARK", "FAILED", "JAVA", "PYTHON"]
                highlights = []
                for line in all_lines[-500:]:
                    if any(key in line.upper() for key in keywords):
                        highlights.append(line.strip())
                
                highlight_text = ""
                if highlights:
                    highlight_text = "── SERVICE ERROR HIGHLIGHTS ──\n" + "\n".join(highlights[-15:]) + "\n\n"
                
                # ── 2. Combine with tail ────────────────────────────────────────
                tail = "── RECENT LOG TAIL ──\n" + "".join(all_lines[-40:])
                return highlight_text + tail

        return "Log file not found at: " + str(log_path)
    except Exception as e:
        return f"Could not retrieve logs: {str(e)}"


def on_task_failure(context) -> None:
    """
    Airflow on_failure_callback.
    Fires immediately when any task fails — BEFORE any retry delay.

    Steps:
      1. Capture full traceback from the exception.
      2. Write ERROR to pipeline_status.json  →  dashboard shows red banner.
      3. Call trigger_error_email()            →  owner receives detailed email.
    """
    ti        = context["task_instance"]
    exception = context.get("exception")

    # ── 1. Build traceback and retrieve logs ──────────────────────────────────
    if exception:
        try:
            import traceback as _tb
            tb_text = "".join(
                _tb.format_exception(type(exception), exception, exception.__traceback__)
            )
        except Exception:
            tb_text = str(exception)
    else:
        tb_text = "No traceback available."

    # Retrieve actual Airflow Task Logs
    task_logs = _read_task_logs(ti)

    exception_msg = str(exception) if exception else "Unknown error"
    attempt       = getattr(ti, "try_number", 1)
    max_attempts  = getattr(ti, "max_tries", 2) + 1

    ti.log.error(
        "[PIPELINE FAILURE] dag=%s task=%s attempt=%s/%s\n%s",
        ti.dag_id, ti.task_id, attempt, max_attempts, tb_text,
    )

    # ── 2. Write shared status file → dashboard picks this up immediately ──────
    write_error(
        dag_id     = ti.dag_id,
        task_id    = ti.task_id,
        run_id     = ti.run_id,
        exception  = exception,
        try_number = attempt,
    )

    # ── 3. Send ERROR email immediately with task logs ────────────────────────
    try:
        trigger_error_email(
            dag_id        = ti.dag_id,
            task_id       = ti.task_id,
            run_id        = ti.run_id,
            attempt       = attempt,
            max_attempts  = max_attempts,
            exception_msg = exception_msg,
            tb_text       = tb_text,
            task_logs     = task_logs,  # PASS LOGS HERE
        )
    except Exception as email_exc:  # noqa: BLE001
        # Never let the notification failure mask the original pipeline error
        ti.log.warning("Error email failed to send: %s", email_exc)


# ── Spark runner ───────────────────────────────────────────────────────────────
def _submit_to_spark(script_path: str, context: dict) -> None:
    """Run a PySpark script inside the running Spark container via Docker SDK."""
    import docker  # lazy import

    ti_log = context["ti"].log
    ti_log.info("[PIPELINE] Submitting → %s", script_path)

    client = docker.from_env()
    try:
        container = client.containers.get("spark")
    except docker.errors.NotFound as exc:
        raise RuntimeError(
            "Spark container 'spark' is not running. "
            "Start it with: docker compose up -d spark"
        ) from exc

    exec_id = client.api.exec_create(
        container.id, SPARK_SUBMIT_CMD + [script_path]
    )["Id"]

    for chunk in client.api.exec_start(exec_id, stream=True):
        for line in chunk.decode("utf-8", errors="replace").splitlines():
            if line.strip():
                ti_log.info(line)

    exit_code = client.api.exec_inspect(exec_id)["ExitCode"]
    ti_log.info("[PIPELINE] Done | exit_code=%s | script=%s", exit_code, script_path)

    if exit_code != 0:
        raise RuntimeError(
            f"spark-submit exited with code {exit_code} for: {script_path}"
        )


def _run_spark_job(**context) -> None:
    _submit_to_spark(SPARK_JOB_PATH, context)


# ── Shared default args ────────────────────────────────────────────────────────
DEFAULT_ARGS = {
    "owner":           "data-eng",
    "depends_on_past": False,
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    # Global fallback — individual tasks override with on_failure_callback
    "on_failure_callback": on_task_failure,
}


# ══════════════════════════════════════════════════════════════════════════════
# DAG 1 — Daily scheduled pipeline
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id      = "daily_pyspark_pipeline",
    description = "Runs the PySpark Delta pipeline daily at 5 AM UTC",
    start_date  = pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule    = "0 5 * * *",
    catchup     = False,
    default_args = DEFAULT_ARGS,
    tags         = ["spark", "delta", "scheduled"],
) as daily_dag:

    d_start = PythonOperator(
        task_id         = "log_pipeline_start",
        python_callable = log_pipeline_start,
    )

    d_spark = PythonOperator(
        task_id              = "run_process_data",
        python_callable      = _run_spark_job,
        on_failure_callback  = on_task_failure,  # ensure immediate callback
    )

    d_finish = PythonOperator(
        task_id         = "log_pipeline_finish",
        python_callable = log_pipeline_finish,
        trigger_rule    = "all_done",            # runs even if upstream failed
    )

    d_start >> d_spark >> d_finish


# ══════════════════════════════════════════════════════════════════════════════
# DAG 2 — Event-driven pipeline (FileSensor)
# ══════════════════════════════════════════════════════════════════════════════
with DAG(
    dag_id      = "event_driven_pyspark_pipeline",
    description = "Triggers when a new file appears in data/input/",
    start_date  = pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule    = "* * * * *",
    catchup     = False,
    default_args = DEFAULT_ARGS,
    tags         = ["spark", "delta", "event-driven", "sensor"],
) as event_dag:

    e_sensor = FileSensor(
        task_id      = "detect_new_input_file",
        fs_conn_id   = FS_CONN_ID,
        filepath     = INPUT_FILE_GLOB,
        poke_interval = 1,
        timeout      = 3600,
        mode         = "poke",
        on_failure_callback = on_task_failure,
    )

    e_start = PythonOperator(
        task_id         = "log_pipeline_start",
        python_callable = log_pipeline_start,
    )

    e_spark = PythonOperator(
        task_id             = "run_process_data",
        python_callable     = _run_spark_job,
        on_failure_callback = on_task_failure,
    )

    e_finish = PythonOperator(
        task_id         = "log_pipeline_finish",
        python_callable = log_pipeline_finish,
        trigger_rule    = "all_done",
    )

    e_sensor >> e_start >> e_spark >> e_finish
