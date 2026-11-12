import boto3
import uuid
import os
import json
import uuid
import os
from datetime import datetime

from botocore.exceptions import ClientError

from generate_sample_data import generate_sample_data

with open(".codesignal/suffix.txt") as f:
    SUFFIX = f.read().strip()
BUCKET_NAME = f"my-data-lake-bucket-{SUFFIX}"  # Must be globally unique

# Initialize S3 client
s3_client = boto3.client("s3")


def create_bucket():
    """Create S3 bucket for data lake"""
    region = s3_client.meta.region_name or "us-east-1"

    try:
        s3_client.head_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} already exists")
        return
    except ClientError:
        pass

    try:
        # us-east-1 is special: do NOT pass CreateBucketConfiguration there
        if region == "us-east-1":
            s3_client.create_bucket(Bucket=BUCKET_NAME)
        else:
            s3_client.create_bucket(
                Bucket=BUCKET_NAME,
                CreateBucketConfiguration={"LocationConstraint": region},
            )
        print(f"Created bucket: {BUCKET_NAME}")

    except ClientError as e:
        code = e.response["Error"].get("Code", "Unknown")
        message = e.response["Error"].get("Message", str(e))
        print(f"Error creating bucket ({code}): {message}")
        raise


def create_folder_structure():
    """Create the data lake folder structure"""
    folders = [
        "raw/user-events/",
        "raw/product-catalog/",
        "processed/user-events/",
        "processed/product-catalog/",
        "curated/analytics/",
        "curated/reports/",
    ]

    for folder in folders:
        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=folder,
                Body="",
            )
            print(f"Created folder: {folder}")

        except ClientError as e:
            code = e.response["Error"].get("Code", "Unknown")
            message = e.response["Error"].get("Message", str(e))
            print(f"Error creating folder {folder} ({code}): {message}")
            raise


def upload_partitioned_data():
    """Upload sample data with time-based partitioning"""
    events = generate_sample_data()
    events_by_date = {}

    for event in events:
        # Parse the timestamp and format for partitioning
        date_obj = datetime.strptime(event["timestamp"], "%Y-%m-%d %H:%M:%S")
        date_key = f"{date_obj.year:04d}/{date_obj.month:02d}/{date_obj.day:02d}"

        if date_key not in events_by_date:
            events_by_date[date_key] = []
        events_by_date[date_key].append(event)

    for date_partition, day_events in events_by_date.items():
        year, month, day = date_partition.split("/")
        partition_key = f"raw/user-events/year={year}/month={month}/day={day}/events.json"
        json_lines = "\n".join([json.dumps(event) for event in day_events])

        try:
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=partition_key,
                Body=json_lines,
                ContentType="application/json",
            )
            print(f"Uploaded {len(day_events)} events to {partition_key}")

        except ClientError as e:
            code = e.response["Error"].get("Code", "Unknown")
            message = e.response["Error"].get("Message", str(e))
            print(f"Error uploading to {partition_key} ({code}): {message}")
            raise


def verify_uploaded_files():
    """Show what files and folders were actually created in S3"""
    try:
        response = s3_client.list_objects_v2(Bucket=BUCKET_NAME)

        print("\nFiles actually created in S3:")
        if 'Contents' in response:
            for obj in response['Contents']:
                print(f"  📁 {obj['Key']} ({obj['Size']} bytes)")
        else:
            print("  No files found")
    except ClientError as e:
        code = e.response["Error"]["Code"]
        message = e.response["Error"]["Message"]
        print(f"Error listing objects: {code} - {message}")
        raise


if __name__ == "__main__":
    print("Setting up Data Lake structure...")
    create_bucket()
    create_folder_structure()
    upload_partitioned_data()
    verify_uploaded_files()