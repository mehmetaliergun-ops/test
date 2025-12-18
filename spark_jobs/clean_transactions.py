import pandas as pd
from sqlalchemy import create_engine
from minio import Minio
import io

# MinIO client
minio_client = Minio(
    "minio:9000",
    access_key="dataopsadmin",
    secret_key="dataopsadmin",
    secure=False
)

bucket = "dataops-bronze"
object_name = "raw/dirty_store_transactions.csv"

# Read CSV from MinIO
data = minio_client.get_object(bucket, object_name)
df = pd.read_csv(io.BytesIO(data.read()))

# Data Cleaning
df = df.drop_duplicates()
df = df.dropna()

# PostgreSQL connection
engine = create_engine(
    "postgresql://airflow:airflow@postgres:5432/traindb"
)

df.to_sql(
    "clean_data_transactions",
    engine,
    schema="public",
    if_exists="replace",
    index=False
)

print("✅ Data cleaning & load completed")
