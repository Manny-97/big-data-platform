from faker import Faker
import pandas as pd
import random
import numpy as np
from typing import Dict, List, Any, Tuple
import concurrent.futures
import time

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
    days_ago = random.randint(0, 365)
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
    
    elapsed = time.time() - start_time
    print(f"Dataset generation completed in {elapsed:.2f} seconds ({total_records/elapsed:.1f} records/sec)")
    
    return df

def main(total_records: int = 1000000, dirty_percentage: float = 20, output_file: str = "telco_data.parquet"):
    df_full = generate_dataset(total_records, dirty_percentage)
    
    print(f"Saving dataset to {output_file}...")
    save_start = time.time()
    df_full.to_parquet(output_file, index=False)
    save_time = time.time() - save_start
    print(f"Dataset saved in {save_time:.2f} seconds")
    
    print(f"Total records: {len(df_full)}")
    
    return df_full

if __name__ == "__main__":
    main(50000, 40, "data/telco_data.parquet")