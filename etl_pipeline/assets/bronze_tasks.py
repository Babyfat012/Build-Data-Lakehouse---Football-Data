import boto3
from botocore.client import Config
import os
import mysql.connector
import io
import pandas as pd
import logging

logging.basicConfig(level=logging.INFO)

# Trả về client S3/MinIO
def get_s3_client():
    endpoint=os.getenv("MINIO_ENDPOINT", "http://minio:9000")
    access_key=os.getenv("MINIO_ACCESS_KEY", "minio")
    secret_key=os.getenv("MINIO_SECRET_KEY", "minio123")

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(s3={"addressing_style": "path"})
    )

# Tạo bucket bronze
def create_bucket_if_not_exists(bucket_name):
    s3 = get_s3_client()
    try:
        existing_buckets = [b['Name'] for b in s3.list_buckets().get('Buckets', [])]
        if bucket_name not in existing_buckets:
            s3.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' đã được tạo.")
        else:
            logging.info(f"Bucket '{bucket_name}' đã tồn tại.")
    except Exception as e:
        logging.error(f"Lỗi khi tạo bucket: {e}")
        raise


def load_csv_to_minio(bucket):
    s3 = get_s3_client()
    csv_dir = "/opt/airflow/app/csv_files"

    if not os.path.exists(csv_dir):
        logging.warning(f"Folder {csv_dir} không tồn tại. Bỏ qua CSV upload.")
        return 
    
    for file in os.listdir(csv_dir):
        if file.endswith(".csv"):   
            file_path= os.path.join(csv_dir, file)
            try:
                s3.upload_file(file_path, bucket, f"bronze/csv/{file}")
                logging.info(f"Uploaded {file} to {bucket}/csv/")
            except Exception as e:
                logging.error(f"Lỗi upload {file}: {e}")
                raise

def load_json_to_minio(bucket):
    s3 = get_s3_client()
    json_dir = "/opt/airflow/app/json_files"

    if not os.path.exists(json_dir):
        logging.warning(f"Folder {json_dir} không tồn tại, bỏ qua JSON upload.")
        return

    for file in os.listdir(json_dir):
        if file.endswith(".json"):
            file_path = os.path.join(json_dir, file)
            try:
                s3.upload_file(file_path, bucket, f"bronze/json/{file}")
                logging.info(f"Uploaded {file} to {bucket}/json/")
            except Exception as e:
                logging.error(f"Lỗi upload {file}: {e}")
                raise


def load_mysql_to_minio(bucket):
    s3 = get_s3_client()
    tables = ["competitions", "clubs", "games", "players"]

    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST", "de_mysql"),
            user=os.getenv("MYSQL_USER", "admin"),
            password=os.getenv("MYSQL_PASSWORD", "admin"),
            database=os.getenv("MYSQL_DB", "football_db")
        )
        for table in tables:
            try:
                df = pd.read_sql(f"SELECT * FROM {table}", conn)
                if df.empty:
                    logging.warning(f"No data found in table {table}")
                    continue

                # Convert df --> CSV (in-memory)
                csv_buffer = io.StringIO()
                df.to_csv(csv_buffer, index=False)

                s3.put_object(
                    Bucket=bucket,
                    Key=f"bronze/mysql/{table}.csv",
                    Body=csv_buffer.getvalue(),
                    ContentType="text/csv"
                )
                logging.info(f"Uploaded {table} to bucket {bucket}/mysql")

            except Exception as e:
                logging.error(f"Lỗi khi xử lý table: {table}: {e}")

    except mysql.connector.Error as err:
        logging.error(f"MySQL connection error: {err}")
    finally:
        if conn is not None and conn.is_connected():
            conn.close()
