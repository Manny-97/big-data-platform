import os

import boto3


def move_py_file_to_s3(file_path, bucket_name, s3_key):
    s3 = boto3.client("s3")

    if not os.path.isfile(file_path):
        print(f"File not found: {file_path}")
        return

    try:
        # Upload file to S3
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"Uploaded {file_path} to s3://{bucket_name}/{s3_key}")

        # Optional: delete the local file after upload
        # os.remove(file_path)
        # print(f"Deleted local file: {file_path}")

    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    move_py_file_to_s3(
        file_path="spark1.py",
        bucket_name="company-pyspark-scripts-223",
        s3_key="uploads/scripts/spark1.py",
    )
