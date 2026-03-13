from pyspark.sql import SparkSession
import os

# Start Spark Session with Delta support
# Adding JVM options for Java 17+ compatibility
spark = SparkSession.builder \
    .appName("VerifyDataPipeline") \
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.permissions.umask-mode", "000") \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED " +
            "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED " +
            "--add-opens=java.base/java.io=ALL-UNNAMED " +
            "--add-opens=java.base/java.net=ALL-UNNAMED " +
            "--add-opens=java.base/java.nio=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED " +
            "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED " +
            "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED " +
            "--add-opens=java.base/sun.security.action=ALL-UNNAMED " +
            "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED " +
            "--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED") \
    .getOrCreate()

# Path to the processed data
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
output_path = os.path.join(project_root, "data", "output", "cleaned_data")

print(f"\n--- Reading Cleaned Data from Delta Lake at {output_path} ---")

if os.path.exists(output_path):
    try:
        # Read the Delta table
        df = spark.read.format("delta").load(output_path)
        
        print("\nSUCCESS: Delta Table Loaded.")
        print(f"Record Count: {df.count()}")
        
        print("\n--- Verified Dataset ---")
        df.show()
        
        # Verify cleaning
        null_count = df.filter(df.age.isNull()).count()
        duplicate_count = df.count() - df.dropDuplicates(["id", "name"]).count()
        
        print(f"\nValidation Results:")
        print(f" - Null Values in Age: {null_count}")
        print(f" - Duplicate Records: {duplicate_count}")
        
        if null_count == 0 and duplicate_count == 0:
            print("\n✅ DATA QUALITY CHECK PASSED!")
        else:
            print("\n❌ DATA QUALITY CHECK FAILED!")
            
    except Exception as e:
        print(f"\n❌ Error Reading Delta Table: {e}")
else:
    print(f"\n❌ Error: Output directory {output_path} does not exist. Run process_data.py first.")

spark.stop()
