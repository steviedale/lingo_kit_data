import boto3
from botocore.exceptions import ClientError
import logging
import os
import requests
import time


def list_s3_objects():
    """
    Lists all objects in a specified S3 bucket.

    Returns:
        list: A list of dictionaries, where each dictionary represents an S3 object
              and contains its key (name), size, and last modified timestamp.
    """
    bucket_name = 'steviedale-language-app' # Replace with your S3 bucket name
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(bucket_name)

    object_list = []
    for obj in bucket.objects.all():
        object_list.append(obj.key)
    return object_list
already_uploaded = list_s3_objects()
print(f"loaded names of {len(already_uploaded)} files that have already been uploaded to s3")


def upload_file(file_path, verbose=False):
    
    """Upload a file to an S3 bucket

    :param file_path: File to upload
    :return: True if file was uploaded, else False
    """

    bucket = 'steviedale-language-app' # Replace with your S3 bucket name
    object_name = os.path.basename(file_path)

    # if file has already been uploaded, don't upload
    if object_name in already_uploaded:
        if verbose:
            print(f"file {object_name} already exists in bucket {bucket}, skipping upload")
        return True

    # raise Exception(
    #     "WARNING: about to upload new audio to s3, did you add new words? " + 
    #     "If you have just added new words, this is expeceted, just comment out this raise Exception line")

    if verbose:
        print('uploading to s3...')
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True