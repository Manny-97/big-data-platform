import boto3
import os
from dotenv import load_dotenv
from botocore.exceptions import ClientError
from faker import Faker
import pandas as pd
import random
import numpy as np
from typing import Dict, List, Any, Tuple
import concurrent.futures
import time
from datetime import datetime,timedelta
import io

def load_aws_credentials():
    """Load AWS credentials from a .env file."""
    load_dotenv()
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    region = os.getenv("AWS_REGION")  # default region
    if not aws_access_key or not aws_secret_key:
        raise ValueError("Missing AWS credentials in .env file.")
    return aws_access_key, aws_secret_key, region

fake = Faker()
random.seed(42)
np.random.seed(42)

CALL_TYPES = ['voice', 'sms']  
DIRECTIONS = ['incoming', 'outgoing']
NETWORK_TYPES = ['2G', '3G', '4G', '5G']
CURRENCIES = ['USD', 'NGN']
NETWORK_PROVIDERS = ['MTN', 'Airtel', 'Glo', '9mobile']
SERVICE_TYPES = ['prepaid', 'postpaid']
ERROR_RATE = 0.05

REGIONS = [fake.state() for _ in range(100)]
CITIES = [fake.city() for _ in range(200)]
CELL_TOWER_IDS = [fake.numerify(text='CT-####') for _ in range(500)]
LOCATION_AREA_CODES = [fake.numerify(text='###-###') for _ in range(300)]

def generate_random_choice(choices: List[Any]) -> Any:
    return random.choice(choices)

def generate_nigerian_phone_number() -> str:
    return '234' + ''.join(str(random.randint(0, 9)) for _ in range(10))

def generate_timestamps() -> Tuple[pd.Timestamp, pd.Timestamp, int]:
    days_ago = random.randint(0,112)
    hours = random.randint(0, 23)
    minutes = random.randint(0, 59)
    seconds = random.randint(0, 59)
    
    start_time = pd.Timestamp.now() - pd.Timedelta(days=days_ago, 
                                                  hours=hours,
                                                  minutes=minutes,
                                                  seconds=seconds)
    duration = random.randint(1, 3600)
    end_time = start_time + pd.Timedelta(seconds=duration)
    return start_time, end_time, duration

def apply_error(value: Any, column: str) -> Any:
    r = random.random()
    if r < ERROR_RATE:
        return None
    
    if column in ['caller_number', 'receiver_number'] and r < ERROR_RATE * 2:
        return 'ERROR' + str(value)[-4:]
    
    if column == 'call_duration_seconds' and r < ERROR_RATE * 2:
        return -abs(value)
    
    return value

def get_call_type_specific_values(call_type: str, duration: int) -> Dict[str, Any]:
    if call_type == 'voice':
        return {
            'call_duration_seconds': duration,
            'message_content': None,
            'message_length': 0
        }
    else:  
        message_length = random.randint(1, 160)
        return {
            'call_duration_seconds': 0,
            'message_content': ''.join(random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz ,.') 
                                      for _ in range(message_length)),
            'message_length': message_length
        }

def generate_base_row() -> Dict[str, Any]:
    return {
        'id': random.randint(1, 9999999),
        'subscriber_id': fake.uuid4(),
        'sim_card_number': '8930' + ''.join(str(random.randint(0, 9)) for _ in range(15)),
        'imsi_number': ''.join(str(random.randint(0, 9)) for _ in range(15)),
        'imei_number': ''.join(str(random.randint(0, 9)) for _ in range(15)),
        'service_type': generate_random_choice(SERVICE_TYPES),
        'direction': generate_random_choice(DIRECTIONS),
        'roaming_status': random.choice([True, False]),
        'caller_number': generate_nigerian_phone_number(),
        'receiver_number': generate_nigerian_phone_number(),
        'country_code': '+234',
        'cell_tower_id': random.choice(CELL_TOWER_IDS),
        'location_latitude': random.uniform(4.0, 14.0),
        'location_longitude': random.uniform(2.0, 15.0),
        'location_area_code': random.choice(LOCATION_AREA_CODES),
        'region': random.choice(REGIONS),
        'city': random.choice(CITIES),
        'network_type': generate_random_choice(NETWORK_TYPES),
        'network_provider': generate_random_choice(NETWORK_PROVIDERS),
        'signal_strength_dbm': random.randint(-120, -50),
        'cost': round(random.uniform(0.01, 10.00), 2),
        'currency': generate_random_choice(CURRENCIES)
    }

def generate_row(with_errors: bool = False) -> Dict[str, Any]:
    call_type = generate_random_choice(CALL_TYPES)  # Only 'voice' or 'sms'
    start_time, end_time, duration = generate_timestamps()
    row = generate_base_row()
    row['call_type'] = call_type
    row['call_start_time'] = start_time
    row['call_end_time'] = end_time
    row['created_at'] = start_time
    row['updated_at'] = end_time
    row.update(get_call_type_specific_values(call_type, duration))
    
    if with_errors:
        row = {k: apply_error(v, k) for k, v in row.items()}
        
    return row

def generate_batch(batch_size: int, with_errors: bool = False) -> List[Dict[str, Any]]:
    return [generate_row(with_errors) for _ in range(batch_size)]

def generate_dataset(total_records: int, dirty_percentage: float, batch_size: int = 10000) -> pd.DataFrame:
    # Critical performance function - handles parallel processing
    start_time = time.time()
    
    # Calculate number of clean and dirty records
    num_dirty = int(total_records * (dirty_percentage / 100))
    num_clean = total_records - num_dirty
    
    print(f"Generating {total_records} records ({num_clean} clean, {num_dirty} dirty)...")
    
    # Prepare batches for parallel processing
    clean_batches = [(batch_size if i < num_clean // batch_size else num_clean % batch_size, False) 
                     for i in range((num_clean + batch_size - 1) // batch_size)]
    
    dirty_batches = [(batch_size if i < num_dirty // batch_size else num_dirty % batch_size, True) 
                     for i in range((num_dirty + batch_size - 1) // batch_size)]
    
    all_batches = clean_batches + dirty_batches
    
    # Process batches in parallel for performance optimization
    all_data = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        future_to_batch = {
            executor.submit(generate_batch, size, is_dirty): (size, is_dirty)
            for size, is_dirty in all_batches if size > 0
        }
        
        for i, future in enumerate(concurrent.futures.as_completed(future_to_batch)):
            batch_size, is_dirty = future_to_batch[future]
            batch_data = future.result()
            all_data.extend(batch_data)
            
            if (i + 1) % 10 == 0 or i + 1 == len(all_batches):
                elapsed = time.time() - start_time
                records_so_far = sum(len(batch) for batch in all_data)
                print(f"Progress: {records_so_far}/{total_records} records ({records_so_far/total_records*100:.1f}%) "
                      f"in {elapsed:.1f} seconds ({records_so_far/elapsed:.1f} records/sec)")
    
    random.shuffle(all_data)
    df = pd.DataFrame(all_data)


    # Convert all datetime64[ns] to datetime64[us] to ensure the data can  be easily deserialized by pyspark
    for col in df.select_dtypes(include=['datetime64[ns]']).columns:
        df[col] = df[col].astype('datetime64[us]')
    
    elapsed = time.time() - start_time
    print(f"Dataset generation completed in {elapsed:.2f} seconds ({total_records/elapsed:.1f} records/sec)")
    
    return df

def create_s3_client():
    """Initialize and return an S3 client."""
    access_key, secret_key, region = load_aws_credentials()
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region
    )

def create_bucket_if_not_exists(s3_client, bucket_name, region):
    """Create the S3 bucket if it doesn't exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")

    # Crete bucket if not found
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"Creating bucket '{bucket_name}'...")

            s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            print("Bucket created.")
               
        else:
            print(e)
            raise(f'Error while creating bucket')     

def upload_file_to_s3(df,s3_client,bucket_name,object_name=None):
    """Upload a file to S3, overwriting if it exists."""
    if object_name is None:
        raise(f'Please Specify the Name of the object to be uploaded')
    
    try:
         # Check if object already exists
        s3_client.head_object(Bucket=bucket_name, Key=object_name)
        raise FileExistsError(f"The object '{object_name}' already exists in bucket '{bucket_name}'.")
    
        # Convert DataFrame to parquet in-memory
    except ClientError as e:
        error_code = int(e.response['Error']['Code'])
        if error_code != 404:
            # Unexpected error other than "Not Found"
            raise e
        
        # Object does not exist if error code is 404, safe to upload
        buffer = io.BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        s3_client.put_object(Bucket=bucket_name, Key=object_name, Body=buffer.getvalue())
        print(f"Uploaded '{object_name}' to 's3://{bucket_name}/{object_name}'.")


def main(object_name,total_records: int = 1000000, dirty_percentage: float = 20,bucket_name = "telecom-synthetic-data"):
    
    df_full = generate_dataset(total_records, dirty_percentage)
    
    # Upload the file to S3
    s3_client = create_s3_client()
    _, _, region = load_aws_credentials()

    create_bucket_if_not_exists(s3_client, bucket_name, region)
    upload_file_to_s3(df_full,s3_client,bucket_name,object_name)

if __name__ == "__main__":
  next_date = (datetime.today() + timedelta(1)).strftime("%Y-%m-%d")
  object_name = f'telco_data_{next_date}.parquet'

  main(object_name,10000000, 40)