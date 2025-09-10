import boto3
from botocore.exceptions import ClientError
import logging
import os
def upload_file(file_path):
    
    """Upload a file to an S3 bucket

    :param file_path: File to upload
    :return: True if file was uploaded, else False
    """

    bucket = 'steviedale-language-app' # Replace with your S3 bucket name
    object_name = os.path.basename(file_path)

    # if file has already been uploaded, don't upload
    url = f"https://{bucket}.s3.us-east-1.amazonaws.com/" + object_name
    response = requests.head(url)
    if response.status_code == 200:
        print(f"file {object_name} already exists in bucket {bucket}, skipping upload")
        return True

    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True