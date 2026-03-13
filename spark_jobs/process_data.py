from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, avg
import os

# Step 4 — Start Spark Session
# We add specific JVM options for compatibility with modern Java versions (17, 21, 25+)
spark = SparkSession.builder \
    .appName("DataEngineeringPipeline") \
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

# Get the absolute path of the current directory to ensure file loading works regardless of where the script is run
script_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(script_dir)
input_path = os.path.join(project_root, "data", "input", "raw_data.csv")
output_path = os.path.join(project_root, "data", "output", "cleaned_data")

# Step 5 — Read the Input Dataset
print("\n--- Step 5: Reading Input Dataset ---")
df = spark.read.csv(
    input_path,
    header=True,
    inferSchema=True
)

# Step 6 — View Data (Debugging)
print("\n--- Step 6: Raw Data ---")
df.show()

# Step 7 — Remove Duplicate Records
print("\n--- Step 7: Removing Duplicates ---")
df = df.dropDuplicates()
df.show()

# Step 8 — Handle Missing Values
print("\n--- Step 8: Handling Missing Values ---")
# Compute average age
average_age_row = df.select(avg("age")).collect()[0][0]
average_age = float(average_age_row) if average_age_row is not None else 0.0

print(f"Computed Average Age: {average_age}")

# Replace null values
df = df.fillna({"age": average_age})
df.show()

# Step 9 — Add Processing Timestamp
print("\n--- Step 9: Adding Processing Timestamp ---")
df = df.withColumn("processed_time", current_timestamp())
df.show()

# Final Step — Send cleaned data to Delta Lake
print(f"\n--- Final Step: Saving Cleaned Data to Delta Lake at {output_path} ---")
df.write.format("delta").mode("overwrite").save(output_path)

print("\nPipeline execution completed successfully!")
