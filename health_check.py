import os
import sys
from pathlib import Path

# Setup path
_PROJECT_ROOT = str(Path(__file__).resolve().parent)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

try:
    print("Checking imports...")
    import pandas as pd
    import plotly.express as px
    import plotly.graph_objects as go
    from deltalake import DeltaTable
    print("Imports OK.")

    import dashboard
    print("Dashboard module imported.")

    script_dir = os.path.dirname(os.path.abspath(__file__))
    input_path = os.path.join(script_dir, "data", "input")
    
    print(f"Testing _compute_medallion_realtime with path: {input_path}")
    bronze, silver, issues = dashboard._compute_medallion_realtime(input_path)
    print(f"Bronze: {len(bronze)} rows, Silver: {len(silver)} rows, Issues: {len(issues)} rows")

    print(f"Testing _compute_gold_from_raw...")
    gold = dashboard._compute_gold_from_raw(input_path)
    print(f"Gold: {len(gold)} rows")

    print("Health check complete. All core functions working.")

except Exception as e:
    print(f"HEALTH CHECK FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
