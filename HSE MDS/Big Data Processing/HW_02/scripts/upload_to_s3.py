from pathlib import Path

import boto3
from botocore.client import Config

DATA_DIR = Path.home() / "HW_02" / "data" / "raw" / "nyctaxi"
BUCKET_NAME = "nyctaxi"

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="tadzhnahal",
        aws_secret_access_key="tadzhnahal123",
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

def create_bucket_if_needed(s3):
    buckets = s3.list_buckets()["Buckets"]
    bucket_names = [bucket["Name"] for bucket in buckets]
    if BUCKET_NAME not in bucket_names:
        s3.create_bucket(Bucket=BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} created")
    else:
        print(f"Bucket {BUCKET_NAME} already exists")

def create_folders(s3):
    folders = ["yellow/", "green/", "fhv/"]

    for folder in folders:
        s3.put_object(Bucket=BUCKET_NAME, Key=folder)
        print(f"Folder {folder} created")

def detect_folder(file_name: str) -> str | None:
    name = file_name.lower()

    if "yellow" in name:
        return "yellow"
    if "green" in name:
        return "green"
    if "for_hire_vehicle" in name or "fhv" in name:
        return "fhv"
    return None

def upload_files(s3):
    for file_path in DATA_DIR.glob("*.csv"):
        folder = detect_folder(file_path.name)

        if folder is None:
            print(f"Skip file: {file_path.name}")
            continue

        key = f"{folder}/{file_path.name}"
        s3.upload_file(str(file_path), BUCKET_NAME, key)
        print(f"Uploaded {file_path.name} -> {key}")

def main():
    s3 = get_s3_client()
    create_bucket_if_needed(s3)
    create_folders(s3)
    upload_files(s3)

if __name__ == "__main__":
    main()