import boto3
import os
from datasets import load_dataset
from huggingface_hub import snapshot_download
from botocore.exceptions import NoCredentialsError, ClientError
import logging
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load the .env file
load_dotenv(override=True)

# Fetch the secrets
bucket_name = os.getenv('S3_BUCKET_NAME', 'gaia-benchmark-data')  # Default to 'gaia-benchmark-data' if not set
aws_access_key_id = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_access_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Debugging step to ensure the bucket name is correctly loaded
if not bucket_name:
    logger.error("Bucket name not found in environment variables.")
else:
    logger.info(f"Using S3 bucket: {bucket_name}")

def upload_file_to_s3(file_path, bucket_name, s3_key):
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key
    )
    try:
        s3_client.upload_file(file_path, bucket_name, s3_key)
        logger.info(f"Uploaded {file_path} to {bucket_name}/{s3_key}")
    except FileNotFoundError:
        logger.error(f"The file {file_path} was not found")
    except NoCredentialsError:
        logger.error("Credentials not available")
    except ClientError as e:
        logger.error(f"Failed to upload {file_path} to S3: {e}")
        raise

def upload_directory_to_s3(directory, bucket_name, base_s3_key):
    for root, dirs, files in os.walk(directory):
        for file in files:
            file_path = os.path.join(root, file)
            # Create the S3 key by preserving the directory structure
            relative_path = os.path.relpath(file_path, directory).replace("\\", "/")
            s3_key = f"{base_s3_key}/{relative_path}"
            logger.info(f"Uploading file: {file_path} to S3 key: {s3_key}")
            upload_file_to_s3(file_path, bucket_name, s3_key)

def main():
    # Load the dataset and save the CSV locally
    try:
        logger.info("Loading dataset...")
        ds = load_dataset("gaia-benchmark/GAIA", "2023_all", download_mode="force_redownload")
    except Exception as e:
        logger.error(f"Failed to load dataset: {e}")
        raise

    csv_test_path = "gaia-test-dataset.csv"
    csv_validation_path = "gaia-validation-dataset.csv"
    
    try:
        logger.info("Saving test and validation datasets to CSV...")
        ds['test'].to_csv(csv_test_path)
        ds['validation'].to_csv(csv_validation_path)
    except Exception as e:
        logger.error(f"Failed to save dataset: {e}")
        raise

    # Download additional files from the Hugging Face repository
    try:
        logger.info("Downloading repository files...")
        repo_id = "gaia-benchmark/GAIA"
        local_dir = snapshot_download(repo_id=repo_id, repo_type="dataset")
        logger.info(f"All files downloaded to: {local_dir}")
    except Exception as e:
        logger.error(f"Failed to download repository files: {e}")
        raise

    # Upload the CSV files to S3
    logger.info(f"Uploading CSV files to S3 bucket: {bucket_name}")
    upload_file_to_s3(csv_test_path, bucket_name, "gaia-dataset/gaia-test-dataset.csv")
    upload_file_to_s3(csv_validation_path, bucket_name, "gaia-dataset/gaia-validation-dataset.csv")

    # Upload the '2023' directory files to S3 with appropriate folders
    logger.info("Uploading 2023 directories to S3...")
    upload_directory_to_s3(os.path.join(local_dir, "2023/test"), bucket_name, "2023/test")
    upload_directory_to_s3(os.path.join(local_dir, "2023/validation"), bucket_name, "2023/validation")

if __name__ == "__main__":
    main()
