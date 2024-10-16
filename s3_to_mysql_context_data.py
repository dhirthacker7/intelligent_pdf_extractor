import boto3
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# AWS Credentials
AWS_ACCESS_KEY_ID = 'XXXXXX'
AWS_SECRET_ACCESS_KEY = 'XXXXXX'

# S3 Configuration
FOLDER_PATH = 'extracts/'
S3_BUCKET_NAME = 'gaia-benchmark-data'

# MySQL database configuration
DB_USERNAME = 'admin'
DB_PASSWORD = 'Assignment2-bdia'
DB_HOST = 'my-s3-import-cluster.ch804m6acz5v.us-east-2.rds.amazonaws.com'
DB_NAME = 'assignment_2'
TARGET_TABLE = 'gaia_merged_pdf'

# Create a session using Boto3
session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY
)

# Create S3 client
s3 = session.client('s3')

# Create a SQLAlchemy engine for MySQL
engine = create_engine(f'mysql+pymysql://{DB_USERNAME}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}')

# Function to get task IDs
def get_task_ids(engine, table_name):
    task_query = text(f"SELECT task_id FROM {table_name}")
    with engine.connect() as connection:
        result = connection.execute(task_query).fetchall()
    return [row[0] for row in result]

# Verify task_id and content before update
def task_id_exists(engine, table_name, task_id):
    check_query = text(f"SELECT COUNT(*) FROM {table_name} WHERE task_id = :task_id;")
    with engine.connect() as connection:
        result = connection.execute(check_query, {"task_id": task_id}).scalar()
    return result > 0

try:
    s3_objects = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=FOLDER_PATH)
    task_ids = get_task_ids(engine, TARGET_TABLE)
    
    if 'Contents' in s3_objects and len(task_ids) > 0:
        task_index = 0
        for obj in s3_objects['Contents']:
            file_key = obj['Key']
            response = s3.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            content = response['Body'].read().decode('utf-8')
            
            if task_index < len(task_ids):
                current_task_id = task_ids[task_index]
                
                if content.strip() == "":
                    print(f"Content for task_id={current_task_id} is empty. Skipping.")
                else:
                    print(f"Attempting to update task_id={current_task_id} with content length={len(content)}")

                    update_query = text(f"""
                        UPDATE {TARGET_TABLE}
                        SET extracted_text = :content
                        WHERE task_id = :task_id;
                    """)
                    with engine.begin() as connection:
                        result = connection.execute(update_query, {"content": content, "task_id": current_task_id})
                        
                    if result.rowcount > 0:
                        print(f"Updated extracted_text for task_id={current_task_id}.")
                    else:
                        print(f"No update made for task_id={current_task_id}.")

                task_index += 1

            if task_index >= len(task_ids):
                break

except SQLAlchemyError as e:
    print(f"SQLAlchemy Error: {e}")
except Exception as e:
    print(f"Error: {e}")
