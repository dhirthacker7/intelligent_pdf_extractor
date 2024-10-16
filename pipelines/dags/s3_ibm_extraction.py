from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
from dotenv import load_dotenv
import os
import logging
import requests
import json
import time

load_dotenv()

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 8),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    's3_pdf_ibm_watson_extraction',
    default_args=default_args,
    description='A DAG to extract text and image information from PDFs using IBM Watson Discovery',
    schedule_interval='@daily',
)

# Initialize logging
logging.basicConfig(level=logging.INFO)

# IBM Watson Discovery credentials
DISCOVERY_API_KEY = os.getenv('DISCOVERY_API_KEY')
DISCOVERY_URL = os.getenv('DISCOVERY_URL')
DISCOVERY_PROJECT_ID = os.getenv('DISCOVERY_PROJECT_ID')
DISCOVERY_COLLECTION_ID = os.getenv('DISCOVERY_COLLECTION_ID')

# Function to list PDF files in both S3 folders
def list_s3_files_combined(**kwargs):
    hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'gaia-benchmark-data'
    prefixes = ['2023/test/', '2023/validation/']
    
    pdf_files = []

    for prefix in prefixes:
        logging.info(f"Listing files in bucket: {bucket_name}, prefix: {prefix}")
        all_files = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        pdf_files.extend([file for file in all_files if file.endswith('.pdf')])

    if not pdf_files:
        logging.warning("No PDF files found.")
    
    ti = kwargs['ti']
    ti.xcom_push(key='pdf_files', value=pdf_files)
    
    return pdf_files

# Function to process PDFs using IBM Watson Discovery
def process_pdf_with_ibm_watson(**kwargs):
    hook = S3Hook(aws_conn_id='aws_default')
    bucket_name = 'gaia-benchmark-data'
    
    # Retrieve the list of PDF files from XCom
    ti = kwargs['ti']
    pdf_files = ti.xcom_pull(task_ids='list_s3_files', key='pdf_files')
    
    if not pdf_files:
        logging.info("No PDF files to process.")
        return
    
    for file_key in pdf_files:
        logging.info(f"Processing file: {file_key}")
        
        try:
            # Download the PDF file from S3
            local_path = '/tmp/' + os.path.basename(file_key)  # Use the original file name
            obj = hook.get_key(file_key, bucket_name=bucket_name)
            obj.download_file(local_path)
            
            # Upload the PDF to Watson Discovery with the original file name
            with open(local_path, 'rb') as pdf_file:
                file_name = os.path.basename(file_key)
                logging.info(f"Uploading PDF to Watson Discovery: {file_key}")
                headers = {
                    'Authorization': f'Bearer {get_watson_token()}'
                }
                files = {'file': (file_name, pdf_file, 'application/pdf')}
                
                # Add the version parameter to the URL
                url = f"{DISCOVERY_URL}/v2/projects/{DISCOVERY_PROJECT_ID}/collections/{DISCOVERY_COLLECTION_ID}/documents?version=2021-06-14"
                
                logging.info(f"Sending request to URL: {url}")
                response = requests.post(url, headers=headers, files=files)
                
                if response.status_code == 202:
                    logging.info(f"Successfully uploaded {file_key} to Watson Discovery.")
                else:
                    logging.error(f"Failed to upload {file_key} to Watson Discovery. Status code: {response.status_code}")
                    continue  # Skip processing this file if upload fails
            
            # Poll for document status
            doc_id = json.loads(response.text)['document_id']
            result_url = f"{DISCOVERY_URL}/v2/projects/{DISCOVERY_PROJECT_ID}/collections/{DISCOVERY_COLLECTION_ID}/documents/{doc_id}?version=2021-06-14"
            headers = {'Authorization': f'Bearer {get_watson_token()}'}
            
            status = "pending"
            retries = 10  # Maximum retries before giving up
            combined_output = ""  # Initialize combined_output to avoid uninitialized variable error
            
            while status == "pending" and retries > 0:
                result_response = requests.get(result_url, headers=headers)
                if result_response.status_code == 200:
                    extracted_data = result_response.json()
                    status = extracted_data['status']
                    logging.info(f"Document {file_key} is in status: {status}")
                    if status == "available":
                        # Document processed successfully
                        combined_output = f"Extracted Text and Image Information:\n{json.dumps(extracted_data, indent=4)}\n"
                        logging.info(f"Extracted Data for {file_key}: {json.dumps(extracted_data, indent=4)}")
                        break
                    elif status == "failed":
                        # Document failed to process
                        combined_output = f"Error: Document {file_key} failed to process.\n"
                        logging.error(f"Document {file_key} failed to process.")
                        break
                else:
                    logging.error(f"Error fetching document status for {file_key}.")
                    combined_output = f"Error fetching document status for {file_key}.\n"
                    break
                
                # Wait and retry if still pending
                retries -= 1
                time.sleep(10)  # Wait 10 seconds before retrying
                
            # Save the combined output back to S3
            output_key = 'ibm_extracts/' + os.path.basename(file_key).replace('.pdf', '_watson_output.txt')
            hook.load_string(combined_output, key=output_key, bucket_name=bucket_name, replace=True)
            
            logging.info(f"Successfully processed and uploaded: {output_key}")
        
        except Exception as e:
            logging.error(f"Error processing file {file_key}: {e}")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

# Function to get IBM Watson Discovery token
def get_watson_token():
    auth_url = f"https://iam.cloud.ibm.com/identity/token"
    data = {
        'grant_type': 'urn:ibm:params:oauth:grant-type:apikey',
        'apikey': DISCOVERY_API_KEY,
    }
    headers = {'Content-Type': 'application/x-www-form-urlencoded'}
    response = requests.post(auth_url, data=data, headers=headers)
    
    if response.status_code == 200:
        return response.json()['access_token']
    else:
        logging.error("Failed to get Watson Discovery token")
        return None

# Task to list files in S3
list_files_task = PythonOperator(
    task_id='list_s3_files',
    python_callable=list_s3_files_combined,
    dag=dag,
)

# Task to process PDFs with IBM Watson Discovery
process_files_task = PythonOperator(
    task_id='process_pdf_with_ibm_watson',
    python_callable=process_pdf_with_ibm_watson,
    dag=dag,
)

# Set the task dependencies
list_files_task >> process_files_task
