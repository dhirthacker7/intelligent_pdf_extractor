from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime
import os
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 10, 8),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    's3_pdf_image_text_extraction',
    default_args=default_args,
    description='A DAG to extract and process text and images from PDFs using PyPDF2, OpenCV, Tesseract, and PyMuPDF',
    schedule_interval='@daily',
)

# Initialize logging
logging.basicConfig(level=logging.INFO)

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

# Function to extract text and images from PDFs using PyPDF2, OpenCV, Tesseract, and PyMuPDF (fitz)
def process_pdf_text_and_images(**kwargs):
    import fitz  # Lazy-load PyMuPDF here to avoid DAG import slowdown
    import cv2
    import pytesseract
    from PyPDF2 import PdfReader  # Lazy-load PyPDF2 here
    
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
            local_path = '/tmp/' + os.path.basename(file_key)
            obj = hook.get_key(file_key, bucket_name=bucket_name)
            obj.download_file(local_path)
            
            # Initialize combined output
            combined_output = ""

            # Extract text using PyPDF2
            reader = PdfReader(local_path)
            text_content = ""
            for page in reader.pages:
                text_content += page.extract_text() or ''
            combined_output += f"Extracted Text from PDF:\n{text_content}\n\n"
            
            # Open the PDF using PyMuPDF (fitz)
            pdf_document = fitz.open(local_path)
            image_count = 0
            
            # Loop through the pages and extract images
            for page_number in range(len(pdf_document)):
                page = pdf_document.load_page(page_number)
                images = page.get_images(full=True)
                
                if images:
                    for img_index, img_info in enumerate(images):
                        xref = img_info[0]
                        base_image = pdf_document.extract_image(xref)
                        image_bytes = base_image["image"]
                        image_extension = base_image["ext"]
                        image_path = f"/tmp/page_{page_number}_img_{img_index}.{image_extension}"
                        
                        # Save image file
                        with open(image_path, "wb") as img_file:
                            img_file.write(image_bytes)

                        # Process the image with OpenCV (basic image manipulation example)
                        img_cv = cv2.imread(image_path)
                        gray_image = cv2.cvtColor(img_cv, cv2.COLOR_BGR2GRAY)
                        _, thresholded_image = cv2.threshold(gray_image, 128, 255, cv2.THRESH_BINARY)
                        opencv_output = f"Processed image {image_path} with OpenCV\n"
                        
                        # Perform OCR on the image using Tesseract
                        ocr_text = pytesseract.image_to_string(gray_image)
                        ocr_output = f"Extracted Text from Image using Tesseract:\n{ocr_text}\n"
                        
                        # Append OpenCV and OCR output to combined_output
                        combined_output += opencv_output + ocr_output
                        
                        # Clean up the image file after processing
                        os.remove(image_path)
                        image_count += 1

            if image_count == 0:
                combined_output += "No images found in the PDF.\n"
            
            # Save the combined output back to S3
            output_key = 'extracts/' + os.path.basename(file_key).replace('.pdf', '_combined_output.txt')
            hook.load_string(combined_output, key=output_key, bucket_name=bucket_name, replace=True)
            
            logging.info(f"Successfully processed and uploaded: {output_key}")
        
        except Exception as e:
            logging.error(f"Error processing file {file_key}: {e}")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

# Task to list files in S3
list_files_task = PythonOperator(
    task_id='list_s3_files',
    python_callable=list_s3_files_combined,
    dag=dag,
)

# Task to process text and images from PDFs
process_files_task = PythonOperator(
    task_id='process_pdf_text_and_images',
    python_callable=process_pdf_text_and_images,
    dag=dag,
)

# Set the task dependencies
list_files_task >> process_files_task
