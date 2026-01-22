from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, unix_timestamp
import boto3, os

# 1. Spark session (ONE time)
spark = SparkSession.builder.appName("TransportETL").getOrCreate()

# 2. Download raw data
s3 = boto3.client("s3")
s3.download_file(
    "transport-raw-data",
    "transport_raw.csv",
    "/tmp/transport_raw.csv"
)

# 3. Read CSV (force local file)
df = spark.read.csv(
    "file:///tmp/transport_raw.csv",
    header=True,
    inferSchema=True
)

print("Rows before cleaning:", df.count())

# 4. Convert to timestamps FIRST
df = df.withColumn(
    "Scheduled_Arrival",
    to_timestamp(col("Scheduled_Arrival"))
)
df = df.withColumn(
    "Actual_Arrival",
    to_timestamp(col("Actual_Arrival"))
)

# 5. Drop nulls ONLY where required
df = df.dropna(subset=["Scheduled_Arrival", "Actual_Arrival"])

print("Rows after cleaning:", df.count())

# 6. Calculate delay
df = df.withColumn(
    "Delay_Minutes",
    (unix_timestamp(col("Actual_Arrival")) -
     unix_timestamp(col("Scheduled_Arrival"))) / 60
)

# 7. Write SINGLE CSV locally
output_path = "/tmp/transport_processed"
df.coalesce(1).write.mode("overwrite").csv(
    f"file://{output_path}",
    header=True
)

# 8. Upload the generated CSV to S3
for file in os.listdir(output_path):
    if file.startswith("part-") and file.endswith(".csv"):
        s3.upload_file(
            f"{output_path}/{file}",
            "transport-processed-data",
            "transport_processed.csv"
        )

spark.stop()
print("Spark ETL completed")
