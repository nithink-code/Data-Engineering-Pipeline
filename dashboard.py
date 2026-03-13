import os
import sys
from datetime import datetime
from pathlib import Path
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
        os.path.join(script_dir, "hadoop"),
        os.path.join(script_dir, ".hadoop"),
        os.environ.get("HADOOP_HOME", ""),
        os.environ.get("hadoop.home.dir", ""),
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


def _compute_gold_from_raw(input_path: str) -> pd.DataFrame:
    """Replicates Spark Gold logic in-memory for instant feedback."""
    import glob
    csv_files = glob.glob(os.path.join(input_path, "*.csv"))
    if not csv_files:
        return pd.DataFrame()

    dfs = []
    for f in csv_files:
        try:
            # Read first few lines to check for headers
            temp_df = pd.read_csv(f)
            
            # Robust Column Mapping
            # 1. Map ID column
            if "order_id" not in temp_df.columns:
                potential_id_cols = ["id", "Order ID", "orderid", "ID"]
                found_id = next((c for c in potential_id_cols if c in temp_df.columns), None)
                if found_id:
                    temp_df.rename(columns={found_id: "order_id"}, inplace=True)
                else:
                    # Fallback: assume first column is the ID if we can't find it
                    temp_df.rename(columns={temp_df.columns[0]: "order_id"}, inplace=True)

            # 2. Map Revenue column
            if "revenue" not in temp_df.columns:
                potential_rev_cols = ["amount", "price", "total", "Revenue", "Revenue_Value"]
                found_rev = next((c for c in potential_rev_cols if c in temp_df.columns), None)
                if found_rev:
                    temp_df.rename(columns={found_rev: "revenue"}, inplace=True)
            
            # 3. Map Date column
            if "order_date" not in temp_df.columns:
                potential_date_cols = ["date", "event_timestamp", "Date", "timestamp"]
                found_date = next((c for c in potential_date_cols if c in temp_df.columns), None)
                if found_date:
                    temp_df.rename(columns={found_date: "order_date"}, inplace=True)
                else:
                    # Fallback: assume last column is date (common in your batches)
                    temp_df.rename(columns={temp_df.columns[-1]: "order_date"}, inplace=True)

            dfs.append(temp_df[["order_id", "order_date"] + ([c for c in ["revenue", "unit_price", "quantity"] if c in temp_df.columns])])
        except Exception:
            continue
    
    if not dfs:
        return pd.DataFrame()
        
    raw_combined = pd.concat(dfs, ignore_index=True)
    
    # 1. Resolve date
    raw_combined["date"] = pd.to_datetime(raw_combined.get("order_date", raw_combined.get("event_timestamp", pd.Timestamp.now())), errors="coerce")
    raw_combined.dropna(subset=["date"], inplace=True)
    raw_combined["date"] = raw_combined["date"].dt.date # Use date only for grouping

    # 2. Resolve revenue
    if "revenue" in raw_combined.columns:
        raw_combined["rev"] = pd.to_numeric(raw_combined["revenue"], errors="coerce").fillna(0.0)
    elif "unit_price" in raw_combined.columns and "quantity" in raw_combined.columns:
        raw_combined["rev"] = raw_combined["unit_price"] * raw_combined["quantity"]
    else:
        raw_combined["rev"] = 0.0

    # 3. Aggregate (Match Gold Schema + New Order IDs column)
    gold_style = (
        raw_combined.groupby("date")
        .agg(
            total_orders=("date", "count"),
            total_revenue=("rev", "sum"),
            order_ids=("order_id", lambda x: ", ".join(x.dropna().astype(str).unique()))
        )
        .reset_index()
    )
    
    # 4. Reorder columns: display order_ids next to date
    cols = ["date", "order_ids", "total_orders", "total_revenue"]
    gold_style = gold_style[cols]
    
    # Cast for performance and consistency
    gold_style["total_orders"] = gold_style["total_orders"].astype("int32")
    gold_style["total_revenue"] = gold_style["total_revenue"].astype("float32")
    
    return gold_style.sort_values("date")


def _normalize_gold_dataframe(raw_df: pd.DataFrame) -> pd.DataFrame:
    """Memory-efficient normalization for large datasets."""
    if raw_df.empty:
        return raw_df

    # 1. Column Mapping (In-place where possible to save memory)
    mapping = {
        "order_date": "date",
        "total_orders_per_day": "total_orders",
        "total_revenue_per_day": "total_revenue",
    }
    
    for old_col, new_col in mapping.items():
        if old_col in raw_df.columns:
            raw_df.rename(columns={old_col: new_col}, inplace=True)
        elif new_col not in raw_df.columns:
            # Add missing columns with optimized defaults
            raw_df[new_col] = 0.0 if "revenue" in new_col else 0

    # 2. Select only needed columns (this creates a slice, then we make it a standalone df)
    final_cols = ["date", "total_orders", "total_revenue"]
    df = raw_df[final_cols].copy() 

    # 3. Optimized Type Casting
    # Convert date efficiently
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df.dropna(subset=["date"], inplace=True)
    
    # Use smaller dtypes to save 50% memory on large numeric scales
    df["total_orders"] = pd.to_numeric(df["total_orders"], errors="coerce").fillna(0).astype("int32")
    df["total_revenue"] = pd.to_numeric(df["total_revenue"], errors="coerce").fillna(0.0).astype("float32")

    return df.sort_values("date")


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
    
    if pandas_df.empty:
        st.warning("Native reader returned an empty dataframe.")

    return _normalize_gold_dataframe(pandas_df)


@st.cache_data(ttl=1, show_spinner=False)
def load_gold_data(gold_path: str) -> pd.DataFrame:
    """
    Load data from the Gold Delta table. 
    Prioritizes the native 'deltalake' reader for speed and stability in Streamlit.
    """
    try:
        # Try the native Delta reader first (recommended for Streamlit/Local dashboard)
        return _load_gold_without_spark(gold_path)
    except Exception as native_exc:
        # If native reader fails, try Spark as a secondary option
        try:
            return _load_gold_with_spark(gold_path)
        except Exception as spark_exc:
            # If both fail, raise the original native error but mention Spark also failed
            raise SparkRuntimePrecheckError(
                f"Failed to load data via native reader ({str(native_exc)[:100]}) "
                f"and Spark fallback also failed ({str(spark_exc)[:100]})."
            )


def inject_shared_styles() -> None:
    st.markdown(
        """
        <style>
        .top-nav-wrap {
            border-radius: 14px;
            padding: 0.7rem 0.9rem;
            margin-bottom: 0.8rem;
            border: 1px solid rgba(14, 165, 233, 0.25);
            background: linear-gradient(90deg, rgba(14,165,233,0.12), rgba(34,197,94,0.12));
        }

        .flow-card {
            position: relative;
            border-radius: 18px;
            padding: 1rem 1rem 0.8rem 1rem;
            margin: 0.7rem 0 1rem 0;
            background: rgba(255, 255, 255, 0.75);
            border: 1px solid rgba(148, 163, 184, 0.35);
            overflow: hidden;
            isolation: isolate;
        }

        .flow-card::before {
            content: "";
            position: absolute;
            inset: -2px;
            border-radius: 18px;
            background: conic-gradient(
                from var(--angle),
                transparent 0deg,
                transparent 220deg,
                var(--line-color) 300deg,
                transparent 360deg
            );
            animation: edge-run 3.2s linear infinite;
            z-index: -1;
        }

        .flow-card::after {
            content: "";
            position: absolute;
            inset: 1px;
            border-radius: 16px;
            background: rgba(255, 255, 255, 0.62);
            z-index: -1;
        }

        .bronze { --line-color: #c27b43; }
        .validation { --line-color: #ef4444; }
        .silver { --line-color: #94a3b8; }

        @property --angle {
            syntax: '<angle>';
            inherits: false;
            initial-value: 0deg;
        }

        @keyframes edge-run {
            from { --angle: 0deg; }
            to { --angle: 360deg; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def render_top_nav() -> str:
    st.markdown('<div class="top-nav-wrap"><strong>Navigation</strong></div>', unsafe_allow_html=True)
    return st.radio(
        "Select view",
        ["Dashboard", "Bronze -> Validation -> Silver"],
        horizontal=True,
        label_visibility="collapsed",
    )


@st.cache_data(ttl=20, show_spinner=False)
def load_delta_df(path_str: str) -> pd.DataFrame:
    from deltalake import DeltaTable  # pyright: ignore[reportMissingImports]

    return DeltaTable(path_str).to_pyarrow_table().to_pandas()


@st.cache_data(ttl=20, show_spinner=False)
def load_delta_history(path_str: str) -> pd.DataFrame:
    from deltalake import DeltaTable  # pyright: ignore[reportMissingImports]

    history_rows: list[dict[str, Any]] = DeltaTable(path_str).history()
    if not history_rows:
        return pd.DataFrame()
    history_df = pd.DataFrame(history_rows)
    if "timestamp" in history_df.columns:
        history_df["timestamp"] = pd.to_datetime(history_df["timestamp"], errors="coerce")
    return history_df


def build_validation_frame(bronze_df: pd.DataFrame, silver_df: pd.DataFrame) -> pd.DataFrame:
    metadata_cols = {"ingestion_timestamp", "source_file", "processing_timestamp"}
    common_cols = [c for c in bronze_df.columns if c in silver_df.columns and c not in metadata_cols]
    if not common_cols:
        return pd.DataFrame()

    checks: list[pd.DataFrame] = []

    duplicate_mask = bronze_df.duplicated(subset=common_cols, keep="first")
    duplicates = bronze_df[duplicate_mask].copy()
    if not duplicates.empty:
        duplicates["Reason"] = "Duplicate Record"
        checks.append(duplicates)

    null_sensitive_cols = [c for c in common_cols if bronze_df[c].isna().any()]
    if null_sensitive_cols:
        null_mask = bronze_df[null_sensitive_cols].isna().any(axis=1)
        null_rows = bronze_df[null_mask & ~duplicate_mask].copy()
        if not null_rows.empty:
            null_rows["Reason"] = "Null values handled or imputed"
            checks.append(null_rows)

    if not checks:
        return pd.DataFrame()

    result = pd.concat(checks, ignore_index=True)
    preferred_cols = [
        c for c in ["order_id", "product", "category", "quantity", "price", "order_date", "Reason"]
        if c in result.columns
    ]
    return result[preferred_cols] if preferred_cols else result


def open_flow_card(step_title: str, subtitle: str, tone: str) -> None:
    st.markdown(
        f"""
        <div class="flow-card {tone}">
            <h3 style="margin:0; color:#000000;">{step_title}</h3>
            <p style="margin:0.25rem 0 0.5rem 0; color:#334155;">{subtitle}</p>
        """,
        unsafe_allow_html=True,
    )


def close_flow_card() -> None:
    st.markdown("</div>", unsafe_allow_html=True)


def render_medallion_section() -> None:
    script_dir = Path(__file__).resolve().parent
    bronze_path = script_dir / "data" / "bronze"
    silver_path = script_dir / "data" / "silver"

    if not bronze_path.exists() or not silver_path.exists():
        st.error("Bronze/Silver Delta paths are missing. Run the pipeline first.")
        return

    try:
        bronze_df = load_delta_df(str(bronze_path))
        bronze_history = load_delta_history(str(bronze_path))
    except Exception as exc:
        st.error(f"Failed to load Bronze: {exc}")
        return

    try:
        silver_df = load_delta_df(str(silver_path))
        silver_history = load_delta_history(str(silver_path))
    except Exception as exc:
        st.error(f"Failed to load Silver: {exc}")
        return

    open_flow_card(
        "Step 1: Bronze Layer (Raw Data)",
        "Ingested source records before quality and dedup transformations.",
        "bronze",
    )
    st.dataframe(bronze_df.head(12), use_container_width=True)
    with st.expander("Show Bronze Delta Transaction History"):
        st.dataframe(bronze_history, use_container_width=True)
    close_flow_card()

    open_flow_card(
        "Step 2: Data Quality and Validation",
        "On-the-fly comparison between Bronze and Silver to show record changes.",
        "validation",
    )
    validation_df = build_validation_frame(bronze_df, silver_df)
    if validation_df.empty:
        st.success("No duplicate/null-handled records detected in the current snapshot.")
    else:
        st.dataframe(validation_df.head(30), use_container_width=True)
    close_flow_card()

    open_flow_card(
        "Step 3: Silver Layer (Cleaned Data)",
        "Deduplicated and null-handled dataset used for downstream analytics.",
        "silver",
    )
    st.dataframe(silver_df.head(12), use_container_width=True)
    with st.expander("Show Silver Delta Transaction History"):
        st.dataframe(silver_history, use_container_width=True)
    close_flow_card()


def render_dashboard_home() -> None:
    st.title("Data Pipeline Analytics Dashboard")

    top_col1, top_col2 = st.columns([6, 1])
    with top_col2:
        if st.button("🔄 Refresh Now"):
            st.cache_data.clear()
            st.rerun()

    # Ultra-fast auto-refresh: every 1 second
    st.markdown(
        """
        <script>
        if (!window.refreshIntervalSet) {
            window.refreshIntervalSet = true;
            setInterval(function() {
                window.location.reload();
            }, 1000);
        }
        </script>
        """,
        unsafe_allow_html=True,
    )

    script_dir = os.path.dirname(os.path.abspath(__file__))
    gold_path = os.path.join(script_dir, "data", "gold")

    raw_input_path = os.path.join(script_dir, "data", "input")
    
    # PERFORMANCE BOOST: Use Hybrid Real-time View
    # We calculate the Gold dataset directly from raw files in memory for 0-latency feedback
    gold_df = _compute_gold_from_raw(raw_input_path)
    
    if gold_df.empty:
        st.warning("No data found in input folder. Please add CSV files to `data/input/`.")
        return

    total_orders = int(gold_df["total_orders"].sum())
    total_revenue = float(gold_df["total_revenue"].sum())

    top_col1, top_col2 = st.columns([6, 1])
    with top_col1:
        st.success("**⚡ Ultra-Fast Real-Time Engine Active** (Showing changes instantly)")

    metric_col_1, metric_col_2 = st.columns(2)
    metric_col_1.metric("Total Living Orders", f"{total_orders:,}")
    metric_col_2.metric("Total Living Revenue", f"{total_revenue:,.2f}")

    st.subheader("Real-Time Revenue Trends")
    st.line_chart(gold_df.set_index("date")["total_revenue"])

    st.subheader("Real-Time Daily Orders")
    st.bar_chart(gold_df.set_index("date")["total_orders"])

    st.subheader("Gold Layer Dataset (Live Preview)")
    if len(gold_df) > 1000:
        st.info(f"Showing top 1000 of {len(gold_df):,} rows.")
        st.dataframe(gold_df.head(1000), use_container_width=True)
    else:
        st.dataframe(gold_df, use_container_width=True)

    st.caption(f"Last heartbeat at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def render_dashboard() -> None:
    inject_shared_styles()
    current_view = render_top_nav()

    if current_view == "Dashboard":
        render_dashboard_home()
    else:
        st.title("Medallion Flow and ACID")
        render_medallion_section()


if __name__ == "__main__":
    render_dashboard()
