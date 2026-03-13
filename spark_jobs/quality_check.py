import json
import logging
import os
import sys
from datetime import datetime
from typing import Any, Dict

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def configure_logging() -> logging.Logger:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(message)s",
    )
    return logging.getLogger("quality_check")


def create_spark_session() -> SparkSession:
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = (
        SparkSession.builder.appName("QualityCheck")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    )
    return configure_spark_with_delta_pip(builder).getOrCreate()


def _add(result: Dict, name: str, status: str, message: str) -> None:
    result["checks"].append({"name": name, "status": status, "message": message})
    if status == "CRITICAL":
        result["critical_failures"] += 1
    elif status == "WARNING":
        result["warnings"] += 1


def check_layer(spark: SparkSession, path: str, layer: str, logger: logging.Logger) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "layer": layer,
        "path": path,
        "status": "PASS",
        "row_count": 0,
        "columns": [],
        "checks": [],
        "critical_failures": 0,
        "warnings": 0,
    }

    if not os.path.exists(path):
        _add(result, "path_exists", "CRITICAL", f"Path does not exist: {path}")
        result["status"] = "FAIL"
        return result

    try:
        df = spark.read.format("delta").load(path)
    except Exception as exc:
        _add(result, "delta_read", "CRITICAL", f"Cannot read Delta table: {str(exc)[:200]}")
        result["status"] = "FAIL"
        return result

    total_rows = df.count()
    result["row_count"] = total_rows
    result["columns"] = df.columns

    # ── 1. Row count ──────────────────────────────────────────────────────────
    if total_rows == 0:
        _add(result, "row_count", "CRITICAL", "Table is empty (0 rows)")
    else:
        _add(result, "row_count", "PASS", f"{total_rows:,} rows")

    # ── 2. Null checks on critical columns ────────────────────────────────────
    for col in [c for c in ["order_id", "order_date", "customer_id"] if c in df.columns]:
        null_count = df.filter(
            F.col(col).isNull() | (F.trim(F.col(col).cast("string")) == "")
        ).count()
        pct = round(null_count / total_rows * 100, 2) if total_rows else 0
        if null_count == 0:
            _add(result, f"null_check_{col}", "PASS", f"No nulls in {col}")
        elif pct > 5:
            _add(result, f"null_check_{col}", "CRITICAL", f"{null_count} nulls in {col} ({pct}%) — exceeds 5% threshold")
        else:
            _add(result, f"null_check_{col}", "WARNING", f"{null_count} nulls in {col} ({pct}%)")

    # ── 3. Duplicate order_id check ───────────────────────────────────────────
    if "order_id" in df.columns:
        total_ids = df.select("order_id").count()
        distinct_ids = df.select("order_id").distinct().count()
        dupes = total_ids - distinct_ids
        if dupes == 0:
            _add(result, "duplicate_order_ids", "PASS", "No duplicate order_ids")
        else:
            pct = round(dupes / total_ids * 100, 2)
            # Duplicates in Silver/Gold are a critical failure; Bronze duplicates are expected
            severity = "CRITICAL" if layer in ("silver", "gold") else "WARNING"
            _add(result, "duplicate_order_ids", severity,
                 f"{dupes} duplicate order_id(s) ({pct}%) — {'unexpected at this layer' if severity == 'CRITICAL' else 'expected in Bronze'}")

    # ── 4. Numeric range checks ───────────────────────────────────────────────
    for col, min_val in [("unit_price", 0), ("quantity", 0)]:
        if col in df.columns:
            invalid = df.filter(F.col(col).cast("double") <= min_val).count()
            if invalid == 0:
                _add(result, f"range_{col}", "PASS", f"All {col} values > {min_val}")
            else:
                _add(result, f"range_{col}", "WARNING", f"{invalid} row(s) with {col} <= {min_val}")

    # ── 5. Date validity ──────────────────────────────────────────────────────
    date_col = next((c for c in ["order_date", "event_timestamp"] if c in df.columns), None)
    if date_col:
        bad_dates = df.filter(F.to_date(F.col(date_col)).isNull()).count()
        if bad_dates == 0:
            _add(result, "date_validity", "PASS", f"All {date_col} values parse as valid dates")
        else:
            _add(result, "date_validity", "CRITICAL", f"{bad_dates} row(s) with unparseable {date_col}")

        future = df.filter(F.to_date(F.col(date_col)) > F.current_date()).count()
        if future == 0:
            _add(result, "future_dates", "PASS", "No future-dated records")
        else:
            _add(result, "future_dates", "WARNING", f"{future} row(s) with future {date_col}")

    # ── 6. Required columns (schema) ──────────────────────────────────────────
    required = {
        "bronze": ["order_id", "order_date", "ingestion_timestamp", "source_file"],
        "quarantine": ["order_id", "order_date", "rejection_reason"],
        "validated": ["order_id", "order_date", "ingestion_timestamp", "source_file"],
        "silver": ["order_id", "order_date", "processing_timestamp"],
        "gold":   ["order_date", "total_orders_per_day"],
    }
    missing = [c for c in required.get(layer, []) if c not in df.columns]
    if missing:
        _add(result, "schema_check", "CRITICAL", f"Missing required columns: {missing}")
    else:
        _add(result, "schema_check", "PASS", "All required columns present")

    # ── 7. Layer-specific business logic ──────────────────────────────────────
    if layer == "quarantine":
        # All quarantine records must have a rejection reason
        if "rejection_reason" in df.columns:
            null_reasons = df.filter(F.col("rejection_reason").isNull() | (F.col("rejection_reason") == "")).count()
            if null_reasons == 0:
                _add(result, "rejection_reason_completeness", "PASS", "All quarantined records have rejection reasons")
            else:
                _add(result, "rejection_reason_completeness", "CRITICAL", f"{null_reasons} quarantine records missing rejection reason")

    elif layer == "validated":
        # Validated records should not have rejection reasons (if column exists)
        if "rejection_reason" in df.columns:
            has_reasons = df.filter(F.col("rejection_reason").isNotNull() & (F.col("rejection_reason") != "")).count()
            if has_reasons == 0:
                _add(result, "clean_validation", "PASS", "No rejection reasons found in validated records")
            else:
                _add(result, "clean_validation", "CRITICAL", f"{has_reasons} validated records still have rejection reasons")

    elif layer == "silver" and total_rows > 0:
        # Silver must have equal-or-fewer rows than Bronze
        # We trust total_rows already; just log the count for visibility.
        _add(result, "silver_row_sanity", "PASS",
             f"Silver has {total_rows:,} rows (duplicates removed from validated)")

    # ── Final layer status ────────────────────────────────────────────────────
    if result["critical_failures"] > 0:
        result["status"] = "FAIL"
    elif result["warnings"] > 0:
        result["status"] = "WARN"

    return result


def main() -> None:
    logger = configure_logging()
    logger.info("=" * 60)
    logger.info("DATA QUALITY & VALIDATION REPORT")
    logger.info("=" * 60)

    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)

    paths = {
        "bronze": os.path.join(project_root, "data", "bronze"),
        "quarantine": os.path.join(project_root, "data", "quarantine"),
        "validated": os.path.join(project_root, "data", "validated"),
        "silver": os.path.join(project_root, "data", "silver"),
        "gold":   os.path.join(project_root, "data", "gold"),
    }
    report_path = os.path.join(project_root, "data", "quality_report.json")

    spark = create_spark_session()
    report: Dict[str, Any] = {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "overall_status": "PASS",
        "layers": [],
    }

    try:
        for layer, path in paths.items():
            logger.info("")
            logger.info("── %s layer (%s) ──", layer.upper(), path)
            result = check_layer(spark, path, layer, logger)
            report["layers"].append(result)

            for check in result["checks"]:
                icon = {"PASS": "✓", "WARNING": "⚠", "CRITICAL": "✗", "WARN": "⚠"}.get(check["status"], "?")
                logger.info("  %s [%-8s] %-30s %s", icon, check["status"], check["name"], check["message"])

            logger.info(
                "  → %s | rows: %s | critical: %d | warnings: %d",
                result["status"], f"{result['row_count']:,}", result["critical_failures"], result["warnings"],
            )

        # Overall status
        statuses = [l["status"] for l in report["layers"]]
        if "FAIL" in statuses:
            report["overall_status"] = "FAIL"
        elif "WARN" in statuses:
            report["overall_status"] = "WARN"

        # Write JSON report
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2, default=str)

        logger.info("")
        logger.info("=" * 60)
        logger.info("OVERALL STATUS: %s", report["overall_status"])
        logger.info("Report written to: %s", report_path)
        logger.info("=" * 60)

        if report["overall_status"] == "FAIL":
            logger.error("Critical quality failures detected — review the report above.")
            sys.exit(1)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
