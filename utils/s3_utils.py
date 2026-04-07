import os
import json
from io import BytesIO, StringIO
import boto3

def get_s3_client():
    return boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION")
    )

def upload_text(text: str, bucket: str, key: str):
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=text.encode("utf-8")
    )

def upload_bytes(data: bytes, bucket: str, key: str):
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=data
    )

def upload_json(obj, bucket: str, key: str):
    payload = json.dumps(obj, indent=2)
    upload_text(payload, bucket, key)

def download_text(bucket: str, key: str) -> str:
    s3 = get_s3_client()
    response = s3.get_object(Bucket=bucket, Key=key)
    return response["Body"].read().decode("utf-8")
