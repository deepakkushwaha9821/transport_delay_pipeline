import pandas as pd
import psycopg2
import boto3
from io import StringIO

s3 = boto3.client("s3")
obj = s3.get_object(Bucket="transport-processed-data", Key="transport_processed.csv")
data = obj["Body"].read().decode("utf-8")

df = pd.read_csv(StringIO(data))

conn = psycopg2.connect(
    host="localhost",
    database="transportdb",
    user="postgres",
    password="deepak123"   # change this
)

cur = conn.cursor()

cur.execute("""
CREATE TABLE IF NOT EXISTS transport_delay(
train_id TEXT,
origin TEXT,
destination TEXT,
scheduled_arrival TIMESTAMP,
actual_arrival TIMESTAMP,
delay_minutes FLOAT
)
""")

for _, row in df.iterrows():
    cur.execute(
        "INSERT INTO transport_delay VALUES (%s,%s,%s,%s,%s,%s)",
        tuple(row)
    )

conn.commit()
cur.close()
conn.close()

print("Loaded into PostgreSQL")
