import os
import sys
from datetime import datetime
from typing import Any

import pandas as pd
import streamlit as st
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


st.set_page_config(page_title="Data Pipeline Analytics Dashboard", layout="wide")


class SparkRuntimePrecheckError(RuntimeError):
    """Raised when local Spark prerequisites are not available."""


def _configure_java_runtime() -> None:
    """Force a Spark-compatible Java runtime when available (Windows-safe)."""
    candidate_paths = [
        os.environ.get("JAVA_HOME", ""),
        r"C:\Users\User\.jdk\jdk-17.0.16",
        r"C:\Program Files\Java\jdk-17",
    ]

    java_home = next((p for p in candidate_paths if p and os.path.exists(p)), None)
    if not java_home:
        return

    os.environ["JAVA_HOME"] = java_home
    java_bin = os.path.join(java_home, "bin")
    current_path = os.environ.get("PATH", "")
    if java_bin not in current_path:
        os.environ["PATH"] = java_bin + os.pathsep + current_path


def _configure_hadoop_runtime() -> None:
    """Configure HADOOP_HOME on Windows when winutils.exe is available."""
    if os.name != "nt":
        return

    script_dir = os.path.dirname(os.path.abspath(__file__))
    candidate_hadoop_homes = [
        os.environ.get("HADOOP_HOME", ""),
        os.environ.get("hadoop.home.dir", ""),
        os.path.join(script_dir, ".hadoop"),
        r"C:\hadoop",
        r"C:\tools\hadoop",
        os.path.join(os.path.expanduser("~"), "hadoop"),
    ]

    selected_home = None
    for home in candidate_hadoop_homes:
        if not home:
            continue

        normalized_home = os.path.abspath(home)
        winutils_path = os.path.join(normalized_home, "bin", "winutils.exe")
        if os.path.exists(winutils_path):
            selected_home = normalized_home
            break

    if selected_home is None:
        raise SparkRuntimePrecheckError(
            "Failed to load Gold Delta table: Spark on Windows requires winutils.exe.\n\n"
            "Fix options:\n"
            "1) Install Hadoop WinUtils and set HADOOP_HOME to that folder (must contain bin\\winutils.exe).\n"
            "2) Place winutils.exe at .hadoop\\bin\\winutils.exe under this project.\n"
            "3) Run Spark jobs in Docker and use the generated Delta output."
        )

    os.environ["HADOOP_HOME"] = selected_home
    os.environ["hadoop.home.dir"] = selected_home

    hadoop_bin = os.path.join(selected_home, "bin")
    current_path = os.environ.get("PATH", "")
    if hadoop_bin not in current_path:
        os.environ["PATH"] = hadoop_bin + os.pathsep + current_path


def create_spark_session() -> SparkSession:
    _configure_java_runtime()
    _configure_hadoop_runtime()

    builder = (
        SparkSession.builder.appName("StreamlitGoldDashboard")
        .config("spark.master", "local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.ivy", os.path.join(os.getcwd(), ".ivy2"))
        .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    )

    # Ensure Spark picks the same Python interpreter as Streamlit.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return configure_spark_with_delta_pip(builder).getOrCreate()


def _normalize_gold_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    if raw_df.empty:
        return raw_df

    renamed_df = raw_df.rename(
        columns={
            "order_date": "date",
            "total_orders_per_day": "total_orders",
            "total_revenue_per_day": "total_revenue",
        }
    )

    for col_name in ["date", "total_orders", "total_revenue"]:
        if col_name not in renamed_df.columns:
            renamed_df[col_name] = 0.0 if col_name == "total_revenue" else 0

    normalized_df = renamed_df[["date", "total_orders", "total_revenue"]].copy()
    normalized_df["date"] = pd.to_datetime(normalized_df["date"], errors="coerce")
    normalized_df["total_orders"] = pd.to_numeric(normalized_df["total_orders"], errors="coerce").fillna(0)
    normalized_df["total_revenue"] = pd.to_numeric(normalized_df["total_revenue"], errors="coerce").fillna(0.0)

    return normalized_df.sort_values("date")


def _load_gold_with_spark(gold_path: str) -> pd.DataFrame:
    spark = create_spark_session()

    spark_df = spark.read.format("delta").load(gold_path)
    if spark_df.rdd.isEmpty():
        return pd.DataFrame()

    return _normalize_gold_dataframe(spark_df.toPandas())


def _load_gold_without_spark(gold_path: str) -> pd.DataFrame:
    try:
        from deltalake import DeltaTable  # pyright: ignore[reportMissingImports]
    except ImportError as exc:
        raise SparkRuntimePrecheckError(
            "Spark is unavailable and Python Delta fallback is not installed. "
            "Install it with: pip install deltalake"
        ) from exc

    delta_table = DeltaTable(gold_path)
    arrow_table: Any = delta_table.to_pyarrow_table()
    pandas_df = arrow_table.to_pandas()

    return _normalize_gold_dataframe(pandas_df)


@st.cache_data(ttl=30, show_spinner=False)
def load_gold_data(gold_path: str) -> pd.DataFrame:
    try:
        return _load_gold_with_spark(gold_path)
    except SparkRuntimePrecheckError:
        # If Spark cannot start on Windows due to missing winutils, use native Delta reader.
        return _load_gold_without_spark(gold_path)


def render_dashboard() -> None:
    st.title("Data Pipeline Analytics Dashboard")

    # Auto-refresh every 30 seconds to pick up Gold layer changes.
    st.caption("Auto-refresh: every 30 seconds")
    st.markdown(
        """
        <script>
        setTimeout(function() {
            window.location.reload();
        }, 30000);
        </script>
        """,
        unsafe_allow_html=True,
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))
    gold_path = os.path.join(script_dir, "data", "gold")

    if not os.path.exists(gold_path):
        st.error(f"Gold layer path not found: {gold_path}")
        st.info("Run the medallion pipeline first to generate `data/gold/`.")
        return

    try:
        gold_df = load_gold_data(gold_path)
    except SparkRuntimePrecheckError as exc:
        st.error(str(exc))
        st.info(
            "After configuring HADOOP_HOME/winutils, restart Streamlit and refresh this page."
        )
        return
    except Exception as exc:
        st.error("Failed to load Gold Delta table.")
        st.info(
            "If this is a Java gateway error, ensure JDK 17 is installed and retry Streamlit. "
            "You can also run the dashboard from a terminal where JAVA_HOME points to JDK 17."
        )
        st.exception(exc)
        return

    if gold_df.empty:
        st.warning("Gold dataset is empty. Please run the pipeline and try again.")
        return

    total_orders = int(gold_df["total_orders"].sum())
    total_revenue = float(gold_df["total_revenue"].sum())

    metric_col_1, metric_col_2 = st.columns(2)
    metric_col_1.metric("Total Orders", f"{total_orders:,}")
    metric_col_2.metric("Total Revenue", f"{total_revenue:,.2f}")

    st.subheader("Total Revenue Over Time")
    st.line_chart(gold_df.set_index("date")["total_revenue"])

    st.subheader("Total Orders Per Day")
    st.bar_chart(gold_df.set_index("date")["total_orders"])

    st.subheader("Gold Layer Dataset")
    st.dataframe(gold_df, use_container_width=True)

    st.caption(f"Last refreshed at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


if __name__ == "__main__":
    render_dashboard()
