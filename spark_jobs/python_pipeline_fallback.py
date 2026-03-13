import os
import pandas as pd
from deltalake.writer import write_deltalake
import datetime

def run_pipeline():
    print("🚀 Starting Pure-Python Data Pipeline Fallback...")
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    input_file = os.path.join(project_root, "data", "input", "raw_data.csv")
    gold_path = os.path.join(project_root, "data", "gold")
    
    # 1. Read Raw
    print(f"📖 Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # 2. Bronze/Silver logic: Clean & Add Metadata
    print("✨ Cleaning and processing data...")
    df = df.drop_duplicates()
    df['ingestion_timestamp'] = datetime.datetime.now()
    
    # Ensure columns exist for Gold aggregation
    if 'order_date' not in df.columns:
        df['order_date'] = pd.to_datetime(df['ingestion_timestamp']).dt.date
    else:
        df['order_date'] = pd.to_datetime(df['order_date']).dt.date
        
    if 'revenue' not in df.columns:
        df['revenue'] = 0.0
    
    # 3. Gold logic: Aggregation
    print("📊 Generating Gold analytics...")
    gold_df = df.groupby('order_date').agg(
        total_orders_per_day=('id', 'count'),
        total_revenue_per_day=('revenue', 'sum')
    ).reset_index()
    
    # Rename for dashboard compatibility
    gold_df = gold_df.rename(columns={'order_date': 'order_date'})
    
    # 4. Write to Delta
    print(f"💾 Writing to Delta Lake at {gold_path}...")
    # Clean up old directory to avoid Spark/Delta checksum conflicts if Spark failed mid-way
    # write_deltalake handles overwrite mode
    write_deltalake(gold_path, gold_df, mode="overwrite")
    
    print("✅ Pipeline completed successfully!")
    print(gold_df)

if __name__ == "__main__":
    run_pipeline()
