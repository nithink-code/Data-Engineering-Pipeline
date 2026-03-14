"""
notifications.py
================
Email notification engine for the Data Engineering Pipeline.

Public API
----------
trigger_error_email(dag_id, task_id, run_id, attempt, exception, tb_text)
    → Called by Airflow on_failure_callback. Sends a detailed red error email.

trigger_success_email(dag_id, run_id, duration_seconds, task_count)
    → Called by Airflow on_success. Sends a clean green success email.

trigger_airflow_alert(context, status)
    → Convenience dispatcher; pass the Airflow callback context directly.

send_pipeline_notification(status, message, details)
    → Generic fallback used by the dashboard buttons and smoke tests.

Environment variables (auto-loaded from .env):
    RESEND_API_KEY  – Your Resend secret key     (required)
    OWNER_EMAIL     – Recipient email address     (required)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path


# ── Auto-load .env from project root ─────────────────────────────────────────
def _load_env() -> None:
    """Load .env using python-dotenv; fall back to a manual line parser."""
    env_path = Path(__file__).resolve().parent.parent / ".env"
    if not env_path.exists():
        return
    try:
        from dotenv import load_dotenv  # type: ignore
        load_dotenv(dotenv_path=env_path, override=False)
    except ImportError:
        with open(env_path, encoding="utf-8") as fh:
            for raw in fh:
                line = raw.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


_load_env()


# ── Shared credentials helper ─────────────────────────────────────────────────
def _credentials() -> tuple[str, str]:
    """Return (api_key, owner_email) or empty strings if not configured."""
    return (
        os.environ.get("RESEND_API_KEY", "").strip(),
        os.environ.get("OWNER_EMAIL", "").strip(),
    )


def _get_resend():
    """Import and configure resend; raises ImportError if not installed."""
    import resend          # type: ignore
    api_key, _ = _credentials()
    resend.api_key = api_key
    return resend


# ══════════════════════════════════════════════════════════════════════════════
# HTML builders
# ══════════════════════════════════════════════════════════════════════════════

def _html_error(
    dag_id: str,
    task_id: str,
    run_id: str,
    attempt: int,
    max_attempts: int,
    exception_msg: str,
    tb_text: str,
    task_logs: str,
    timestamp: str,
) -> str:
    tb_block = ""
    if tb_text:
        tb_block = f"""
        <div style="margin-top:20px; background:#1e293b; border-radius:8px; padding:16px; border:1px solid #334155;">
            <p style="margin:0 0 8px; font-weight:700; color:#f87171; font-size:13px; text-transform:uppercase; letter-spacing:0.5px;">
                Python Traceback
            </p>
            <pre style="margin:0; white-space:pre-wrap; word-break:break-word;
                        font-size:12px; color:#cbd5e1; line-height:1.6; font-family:Consolas, monospace;">{tb_text}</pre>
        </div>"""

    log_block = ""
    if task_logs:
        # Check if the logs contain our special header for highlights
        if "── SERVICE ERROR HIGHLIGHTS ──" in task_logs:
            parts = task_logs.split("── RECENT LOG TAIL ──")
            highlights = parts[0].strip()
            tail = parts[1].strip() if len(parts) > 1 else ""
            
            log_block = f"""
            <div style="margin-top:20px; border:1px solid #334155; border-radius:8px; overflow:hidden;">
                <div style="background:#450a0a; padding:10px 16px; border-bottom:1px solid #334155;">
                    <p style="margin:0; font-weight:800; color:#f87171; font-size:12px; text-transform:uppercase; letter-spacing:1px;">
                        🎯 Service Error Highlights
                    </p>
                </div>
                <div style="background:#0f172a; padding:16px;">
                    <pre style="margin:0; white-space:pre-wrap; word-break:break-word;
                                font-size:12px; color:#fca5a5; line-height:1.5; font-family:Consolas, monospace;">{highlights}</pre>
                </div>
                <div style="background:#1e293b; padding:8px 16px; border-top:1px solid #334155;">
                    <p style="margin:0; font-weight:700; color:#94a3b8; font-size:11px; text-transform:uppercase;">Recent Log Context</p>
                </div>
                <div style="background:#0f172a; padding:16px;">
                    <pre style="margin:0; white-space:pre-wrap; word-break:break-word;
                                font-size:11px; color:#64748b; line-height:1.4; font-family:Consolas, monospace;">{tail}</pre>
                </div>
            </div>"""
        else:
            log_block = f"""
            <div style="margin-top:16px; background:#0f172a; border-radius:8px; padding:16px; border:1px solid #1e293b;">
                <p style="margin:0 0 8px; font-weight:700; color:#38bdf8; font-size:13px; text-transform:uppercase;">
                    Airflow Task Logs
                </p>
                <pre style="margin:0; white-space:pre-wrap; word-break:break-word;
                            font-size:11px; color:#94a3b8; line-height:1.4; font-family:Consolas, monospace;">{task_logs}</pre>
            </div>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:36px 16px;">
      <table width="620" cellpadding="0" cellspacing="0"
             style="border-radius:14px;overflow:hidden;
                    box-shadow:0 8px 32px rgba(0,0,0,0.5);">

        <!-- RED HEADER -->
        <tr>
          <td style="background:linear-gradient(135deg,#dc2626,#b91c1c);
                     padding:28px 36px;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <p style="margin:0 0 4px;font-size:13px;
                             color:rgba(255,255,255,0.7);letter-spacing:1px;">
                    DATA ENGINEERING PIPELINE
                  </p>
                  <h1 style="margin:0;color:#fff;font-size:26px;font-weight:800;">
                    ❌ Pipeline Failure
                  </h1>
                </td>
                <td align="right" valign="top">
                  <span style="background:rgba(0,0,0,0.25);padding:6px 14px;
                               border-radius:20px;color:#fca5a5;
                               font-size:13px;font-weight:700;">
                    ERROR
                  </span>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- BODY -->
        <tr>
          <td style="background:#1e293b;padding:28px 36px;">

            <!-- Key facts grid -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="margin-bottom:20px;border-collapse:collapse;">
              <tr>
                <td width="50%" style="padding:10px 14px;background:#0f172a;
                                        border-radius:8px 0 0 8px;
                                        border:1px solid #334155;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">DAG</p>
                  <p style="margin:4px 0 0;font-size:15px;font-weight:700;
                             color:#e2e8f0;">{dag_id}</p>
                </td>
                <td width="50%" style="padding:10px 14px;background:#0f172a;
                                        border-radius:0 8px 8px 0;
                                        border:1px solid #334155;border-left:none;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">FAILED TASK</p>
                  <p style="margin:4px 0 0;font-size:15px;font-weight:700;
                             color:#f87171;">{task_id}</p>
                </td>
              </tr>
              <tr><td colspan="2" style="height:8px;"></td></tr>
              <tr>
                <td style="padding:10px 14px;background:#0f172a;
                            border-radius:8px 0 0 8px;
                            border:1px solid #334155;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">ATTEMPT</p>
                  <p style="margin:4px 0 0;font-size:15px;font-weight:700;
                             color:#fbbf24;">{attempt} of {max_attempts}</p>
                </td>
                <td style="padding:10px 14px;background:#0f172a;
                            border-radius:0 8px 8px 0;
                            border:1px solid #334155;border-left:none;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">TIME</p>
                  <p style="margin:4px 0 0;font-size:13px;font-weight:600;
                             color:#94a3b8;">{timestamp}</p>
                </td>
              </tr>
            </table>

            <!-- Exception summary -->
            <div style="background:#450a0a;border:1px solid #dc2626;
                        border-radius:8px;padding:14px 16px;margin-bottom:4px;">
              <p style="margin:0 0 6px;font-size:12px;color:#f87171;
                         font-weight:700;text-transform:uppercase;letter-spacing:0.8px;">
                Exception
              </p>
              <p style="margin:0;font-size:14px;color:#fecaca;
                         word-break:break-word;">{exception_msg}</p>
            </div>

            <p style="margin:4px 0 0;font-size:12px;color:#475569;">
              Run ID: <code style="color:#94a3b8;">{run_id}</code>
            </p>

            {tb_block}
            {log_block}
          </td>
        </tr>

        <!-- FOOTER -->
        <tr>
          <td style="background:#0f172a;padding:16px 36px;
                     border-top:1px solid #1e293b;">
            <p style="margin:0;font-size:12px;color:#475569;">
              🤖 Sent automatically by
              <strong style="color:#64748b;">Data-Engineering-Pipeline</strong>
              via Resend &nbsp;·&nbsp; {timestamp}
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def _html_success(
    dag_id: str,
    run_id: str,
    duration_seconds: float,
    task_count: int,
    timestamp: str,
) -> str:
    duration_str = (
        f"{int(duration_seconds // 60)}m {int(duration_seconds % 60)}s"
        if duration_seconds >= 60
        else f"{duration_seconds:.1f}s"
    )
    return f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#0f172a;font-family:'Segoe UI',Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:36px 16px;">
      <table width="620" cellpadding="0" cellspacing="0"
             style="border-radius:14px;overflow:hidden;
                    box-shadow:0 8px 32px rgba(0,0,0,0.5);">

        <!-- GREEN HEADER -->
        <tr>
          <td style="background:linear-gradient(135deg,#16a34a,#15803d);
                     padding:28px 36px;">
            <table width="100%" cellpadding="0" cellspacing="0">
              <tr>
                <td>
                  <p style="margin:0 0 4px;font-size:13px;
                             color:rgba(255,255,255,0.7);letter-spacing:1px;">
                    DATA ENGINEERING PIPELINE
                  </p>
                  <h1 style="margin:0;color:#fff;font-size:26px;font-weight:800;">
                    ✅ Pipeline Completed
                  </h1>
                </td>
                <td align="right" valign="top">
                  <span style="background:rgba(0,0,0,0.2);padding:6px 14px;
                               border-radius:20px;color:#bbf7d0;
                               font-size:13px;font-weight:700;">
                    SUCCESS
                  </span>
                </td>
              </tr>
            </table>
          </td>
        </tr>

        <!-- BODY -->
        <tr>
          <td style="background:#1e293b;padding:28px 36px;">

            <!-- Stats grid -->
            <table width="100%" cellpadding="0" cellspacing="0"
                   style="margin-bottom:20px;border-collapse:collapse;">
              <tr>
                <td width="33%" style="padding:12px 14px;background:#0f172a;
                                        border-radius:8px 0 0 8px;
                                        border:1px solid #334155;text-align:center;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">DAG</p>
                  <p style="margin:6px 0 0;font-size:14px;font-weight:700;
                             color:#e2e8f0;">{dag_id}</p>
                </td>
                <td width="33%" style="padding:12px 14px;background:#0f172a;
                                        border:1px solid #334155;border-left:none;
                                        text-align:center;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">TASKS</p>
                  <p style="margin:6px 0 0;font-size:22px;font-weight:800;
                             color:#4ade80;">{task_count}</p>
                </td>
                <td width="33%" style="padding:12px 14px;background:#0f172a;
                                        border-radius:0 8px 8px 0;
                                        border:1px solid #334155;border-left:none;
                                        text-align:center;">
                  <p style="margin:0;font-size:11px;color:#64748b;
                             text-transform:uppercase;letter-spacing:0.8px;">DURATION</p>
                  <p style="margin:6px 0 0;font-size:18px;font-weight:700;
                             color:#86efac;">{duration_str}</p>
                </td>
              </tr>
            </table>

            <!-- Success message -->
            <div style="background:#052e16;border:1px solid #16a34a;
                        border-radius:8px;padding:16px 20px;">
              <p style="margin:0;font-size:15px;color:#bbf7d0;line-height:1.6;">
                All tasks in <strong>{dag_id}</strong> finished successfully.
                Your data has been processed through the full
                <strong>Bronze → Silver → Gold</strong> Medallion pipeline.
              </p>
            </div>

            <p style="margin:16px 0 0;font-size:12px;color:#475569;">
              Run ID: <code style="color:#94a3b8;">{run_id}</code><br/>
              Completed at: <strong style="color:#64748b;">{timestamp}</strong>
            </p>
          </td>
        </tr>

        <!-- FOOTER -->
        <tr>
          <td style="background:#0f172a;padding:16px 36px;
                     border-top:1px solid #1e293b;">
            <p style="margin:0;font-size:12px;color:#475569;">
              🤖 Sent automatically by
              <strong style="color:#64748b;">Data-Engineering-Pipeline</strong>
              via Resend &nbsp;·&nbsp; {timestamp}
            </p>
          </td>
        </tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


# ══════════════════════════════════════════════════════════════════════════════
# Public trigger functions
# ══════════════════════════════════════════════════════════════════════════════

def trigger_error_email(
    dag_id: str,
    task_id: str,
    run_id: str,
    attempt: int = 1,
    max_attempts: int = 3,
    exception_msg: str = "Unknown error",
    tb_text: str = "",
    task_logs: str = "",
) -> bool:
    """
    Send a rich red ERROR email immediately when an Airflow task fails.

    Called automatically by on_failure_callback in the DAG.

    Returns True if the email was sent, False otherwise.
    """
    api_key, owner_email = _credentials()
    if not api_key or not owner_email:
        print("⚠️  trigger_error_email: credentials not set – skipped.", file=sys.stderr)
        return False

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = (
        f"❌ [PIPELINE ERROR] {dag_id} · Task '{task_id}' failed "
        f"(attempt {attempt}) · {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    html = _html_error(dag_id, task_id, run_id, attempt, max_attempts,
                       exception_msg, tb_text, task_logs, ts)

    try:
        resend = _get_resend()
        resp = resend.Emails.send({
            "from":    "Pipeline-Bot <onboarding@resend.dev>",
            "to":      [owner_email],
            "subject": subject,
            "html":    html,
        })
        print(f"✅ Error email sent  → id={resp.get('id')}  to={owner_email}")
        return True
    except Exception as exc:
        print(f"❌ trigger_error_email failed: {exc}", file=sys.stderr)
        return False


def trigger_success_email(
    dag_id: str,
    run_id: str,
    duration_seconds: float = 0.0,
    task_count: int = 0,
) -> bool:
    """
    Send a clean green SUCCESS email when a DAG run finishes without errors.

    Called automatically by log_pipeline_finish in the DAG.

    Returns True if the email was sent, False otherwise.
    """
    api_key, owner_email = _credentials()
    if not api_key or not owner_email:
        print("⚠️  trigger_success_email: credentials not set – skipped.", file=sys.stderr)
        return False

    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject = (
        f"✅ [PIPELINE SUCCESS] {dag_id} completed · "
        f"{datetime.now().strftime('%Y-%m-%d %H:%M')}"
    )
    html = _html_success(dag_id, run_id, duration_seconds, task_count, ts)

    try:
        resend = _get_resend()
        resp = resend.Emails.send({
            "from":    "Pipeline-Bot <onboarding@resend.dev>",
            "to":      [owner_email],
            "subject": subject,
            "html":    html,
        })
        print(f"✅ Success email sent → id={resp.get('id')}  to={owner_email}")
        return True
    except Exception as exc:
        print(f"❌ trigger_success_email failed: {exc}", file=sys.stderr)
        return False


def trigger_airflow_alert(context: dict, status: str = "ERROR") -> None:
    """
    Convenience dispatcher for Airflow callback context dicts.

    Usage in DAG:
        on_failure_callback = lambda ctx: trigger_airflow_alert(ctx, "ERROR")
        on_success_callback = lambda ctx: trigger_airflow_alert(ctx, "SUCCESS")
    """
    import traceback as _traceback

    ti = context.get("task_instance") or context.get("ti")
    if ti is None:
        return

    dag_id     = ti.dag_id
    task_id    = ti.task_id
    run_id     = ti.run_id
    attempt    = getattr(ti, "try_number", 1)
    max_tries  = getattr(ti, "max_tries", 2) + 1

    if status.upper() == "ERROR":
        exception = context.get("exception")
        if exception:
            try:
                tb_lines = _traceback.format_exception(
                    type(exception), exception, exception.__traceback__
                )
                tb_text = "".join(tb_lines)
            except Exception:
                tb_text = str(exception)
        else:
            tb_text = "No traceback available."

        trigger_error_email(
            dag_id        = dag_id,
            task_id       = task_id,
            run_id        = run_id,
            attempt       = attempt,
            max_attempts  = max_tries,
            exception_msg = str(exception) if exception else "Unknown error",
            tb_text       = tb_text,
        )

    elif status.upper() == "SUCCESS":
        # Compute wall-clock duration if Airflow provides start_date
        start_date = getattr(ti, "start_date", None)
        duration   = (datetime.utcnow() - start_date).total_seconds() if start_date else 0.0

        # Count tasks in this DAG run
        try:
            from airflow.models import DagRun  # type: ignore
            dag_run    = context.get("dag_run")
            task_count = len(dag_run.get_task_instances()) if dag_run else 0
        except Exception:
            task_count = 0

        trigger_success_email(
            dag_id           = dag_id,
            run_id           = run_id,
            duration_seconds = duration,
            task_count       = task_count,
        )


# ── Generic fallback (used by dashboard buttons / smoke tests) ────────────────
def send_pipeline_notification(
    status: str,
    message: str,
    details: str = "",
) -> bool:
    """
    Generic email sender used by the dashboard sidebar buttons and smoke tests.
    For Airflow callbacks, prefer trigger_error_email / trigger_success_email.
    """
    api_key, owner_email = _credentials()
    if not api_key:
        print("⚠️  RESEND_API_KEY not set – email skipped.", file=sys.stderr)
        return False
    if not owner_email:
        print("⚠️  OWNER_EMAIL not set – email skipped.", file=sys.stderr)
        return False

    _STATUS_COLOR = {"SUCCESS":"#22c55e","ERROR":"#ef4444","WARNING":"#f97316",
                     "START":"#3b82f6","TEST":"#8b5cf6"}
    _STATUS_ICON  = {"SUCCESS":"✅","ERROR":"❌","WARNING":"⚠️","START":"🚀","TEST":"🧪"}

    color = _STATUS_COLOR.get(status.upper(), "#64748b")
    icon  = _STATUS_ICON.get(status.upper(), "ℹ️")
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    details_block = (
        f"""<div style="margin-top:16px;background:#f1f5f9;border:1px solid #e2e8f0;
                border-radius:6px;padding:12px;">
            <p style="margin:0 0 6px;font-weight:600;color:#475569;">Details / Logs</p>
            <pre style="margin:0;white-space:pre-wrap;font-size:13px;
                        color:#334155;">{details}</pre></div>"""
        if details else ""
    )
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"/></head>
<body style="margin:0;padding:0;background:#f1f5f9;font-family:Arial,sans-serif;">
  <table width="100%" cellpadding="0" cellspacing="0">
    <tr><td align="center" style="padding:32px 16px;">
      <table width="600" style="background:#fff;border-radius:12px;
                                box-shadow:0 4px 24px rgba(0,0,0,0.08);overflow:hidden;">
        <tr><td style="background:{color};padding:24px 32px;">
            <h1 style="margin:0;color:#fff;font-size:22px;">{icon} Data Pipeline — {status}</h1>
        </td></tr>
        <tr><td style="padding:28px 32px;">
            <p style="margin:0 0 10px;font-size:15px;color:#1e293b;">
                <strong>Message:</strong> {message}</p>
            <p style="margin:0;font-size:13px;color:#64748b;">
                <strong>Time:</strong> {ts}</p>
            {details_block}
        </td></tr>
        <tr><td style="background:#f8fafc;padding:16px 32px;border-top:1px solid #e2e8f0;">
            <p style="margin:0;font-size:12px;color:#94a3b8;">
                Sent automatically by <strong>Data-Engineering-Pipeline</strong>
                via Resend · {ts}</p>
        </td></tr>
      </table></td></tr></table></body></html>"""

    try:
        resend = _get_resend()
        resp = resend.Emails.send({
            "from":    "Pipeline-Bot <onboarding@resend.dev>",
            "to":      [owner_email],
            "subject": f"[{status}] Data Pipeline Alert – {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            "html":    html,
        })
        print(f"✅ Email sent → id={resp.get('id')}  to={owner_email}")
        return True
    except Exception as exc:
        print(f"❌ Resend send failed: {exc}", file=sys.stderr)
        return False


# ── Smoke tests ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse, sys as _sys

    parser = argparse.ArgumentParser(description="Test pipeline email notifications")
    parser.add_argument("--type", choices=["error", "success", "generic"],
                        default="generic", help="Email type to test")
    args = parser.parse_args()

    if args.type == "error":
        ok = trigger_error_email(
            dag_id        = "daily_pyspark_pipeline",
            task_id       = "run_process_data",
            run_id        = "manual__test__001",
            attempt       = 1,
            max_attempts  = 3,
            exception_msg = "RuntimeError: spark-submit exited with code 1",
            tb_text       = "Traceback (most recent call last):\n  File 'test.py', line 1\n    raise RuntimeError('spark exit code 1')\nRuntimeError: spark-submit exited with code 1",
            task_logs     = "[2026-03-14 06:15:01] INFO - Starting spark job...\n[2026-03-14 06:15:05] ERROR - java.io.FileNotFoundException: data/input/not_found.csv\n[2026-03-14 06:15:06] INFO - Cleaning up temporary files...",
        )
    elif args.type == "success":
        ok = trigger_success_email(
            dag_id           = "daily_pyspark_pipeline",
            run_id           = "manual__test__001",
            duration_seconds = 142.5,
            task_count       = 4,
        )
    else:
        ok = send_pipeline_notification(
            status  = "TEST",
            message = "Smoke test from notifications.py – everything looks good.",
            details = f"Recipient: {os.environ.get('OWNER_EMAIL', 'NOT SET')}",
        )

    _sys.exit(0 if ok else 1)
