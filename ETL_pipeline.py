import os
import time
import logging
import glob
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, to_timestamp,max
from datetime import datetime, timedelta, timezone
import boto3
import sys
from pyspark.sql.utils import AnalysisException


# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)
log_filename = datetime.now().strftime("logs/log_%Y-%m-%d_%H-%M-%S.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
)

# logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
# logger = logging.getLogger("ETL_Telco")

logger = logging.getLogger(__name__)

def load_environment():
    """Load and return necessary environment variables."""
    load_dotenv()
    env_type = os.getenv("ENVIRONMENT", None)  # None means production
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")
    jar_path = os.getenv("SPARK_JAR_PATH")
    base_path = os.getenv("BASE_FILE_PATH")
    source_bucket = os.getenv("SOURCE_BUCKET")
    target_path =  os.getenv("TARGET_PATH")

    return env_type, aws_access_key, aws_secret_key, aws_region, jar_path,base_path,source_bucket,target_path


def create_spark_connection(env_type, aws_access_key, aws_secret_key, aws_region, jar_path):
    """Create Spark session based on environment (local or EMR)."""
    try:
        conf = SparkConf()

        if env_type == "local":
            if not all([aws_access_key, aws_secret_key, aws_region, jar_path]):
                raise ValueError("Missing required environment variables for local development.")

            conf.set("spark.hadoop.fs.s3a.access.key", aws_access_key) \
                .set("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
                .set("spark.hadoop.fs.s3a.endpoint", f"s3.{aws_region}.amazonaws.com") \
                .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
                .set("spark.driver.extraClassPath", f"{jar_path}hadoop-aws-3.3.4.jar,{jar_path}aws-java-sdk-bundle-1.12.353.jar").set("spark.hadoop.fs.s3a.fast.upload", "true") 

            master = "local[6]"
        else:
            master = None  # EMR cluster

        conf.set("spark.sql.adaptive.enabled", "true") \
            .set("spark.sql.adaptive.skewJoin.enabled", "true") \
            .set("spark.sql.files.maxPartitionBytes", str(200 * 1024 * 1024))
        

        # Define default builder
        builder = SparkSession.builder.appName("Processing_telcom_data").config(conf=conf)

        if master:
            builder = builder.master(master)

        spark = builder.getOrCreate()
        logger.info("Spark session created.")
        return spark

    except Exception as e:
        logger.error(f" Error creating Spark session: {e}")
        raise

## Get the list of files uploaded every last 24 hours
def get_uploaded_files(spark,aws_access_key: str, aws_secret_key: str,source_bucket: str,target_path: str):
    
    try:
        logger.info("Connecting to S3 bucket to list files...")
        
        s3 = boto3.client('s3', aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key)

        # Check if target dataset exists (First Run Logic)
        first_run = False
        try:
            spark.read.parquet(target_path)
            logger.info("Existing target dataset found. Proceeding with incremental load.")
        except AnalysisException:
            logger.warning("Target dataset not found. Assuming FIRST RUN — full load will be performed.")
            first_run = True

        # List objects in the bucket
        response = s3.list_objects_v2(Bucket=source_bucket)

        if 'Contents' not in response:
            logger.warning(f"No files found in bucket '{source_bucket}'.")
            return []

        # If first run → Process **all files**
        if first_run:
            file_list = [
                f"s3a://{source_bucket}/{obj['Key']}"
                for obj in response['Contents']
            ]
            logger.info(f"First run: Found {len(file_list)} total files to process.")
        
        # If not first run → Process **files uploaded in last 24 hours**
        else:
            start_time = datetime.now(timezone.utc) - timedelta(days=1)
            file_list = [
                f"s3a://{source_bucket}/{obj['Key']}"
                for obj in response['Contents']
                if obj['LastModified'] > start_time
            ]
            logger.info(f"Found {len(file_list)} new files uploaded in the last 24 hours.")
        
        return file_list
    
    except Exception as e:
        logger.error(f"Failed to fetch file list from S3: {e}")
        raise


def read_data_from_s3(spark, source_files: list):
   
    """Read multiple datasets using Spark."""
    try:
        if not source_files:
            logger.warning("No new files detected. Skipping data read.")
            return None

        logger.info(f"Starting to read {len(source_files)} parquet files...")
        start_time = time.time()

        df = spark.read.parquet(*source_files)

        logger.info(f"Successfully read {df.count()} records from {len(source_files)} files in {time.time() - start_time:.2f} seconds.")
        return df

    except Exception as e:
        logger.error(f"Failed to read data from files: {e}")
        raise


def transform_data(spark,df,target_path):

    """ Clean and transform the data accordingly"""
    if df is None:
        logger.info("No data to process. Exiting gracefully.")
        spark.stop()
        sys.exit(0)
               # Exit the Python process with success code

    # Process only new and updated records
    try:
        # Read The Existing data
        logger.info(f"Attempting to read existing target dataset from {target_path}...")

        try:
            target_df = spark.read.parquet(target_path)
            target_record_count = target_df.count()
            logger.info(f"Successfully read {target_record_count} records from target dataset.")
            is_first_run = False

        except AnalysisException as e:
            logger.warning(f"Target dataset not found. Performing full load. Details: {e}")
            target_df = None
            is_first_run = True
        
        if is_first_run:
            logger.info("First run detected. Processing all incoming records.")
            new_changed_entries = df


        else:

            # Identify latest timestamp from target
            latest_ts= target_df.select(max("updated_at")).collect()[0][0]

            # Filter new or updated entries
            new_changed_entries = df.filter(col("updated_at") > latest_ts)

            # Identify IDs of new/changed records
            new_changed_id = new_changed_entries.select("id").distinct()

            # Extract unchanged records from existing target
            unchanged_entries_df = target_df.join(new_changed_id,"id", "left_anti")

            logger.info(f"{unchanged_entries_df.count()} unchanged records retained.")


        logger.info(f'Starting cleaning {new_changed_entries.count()} new records')

        # casting column to appropriate type
        new_changed_entries = new_changed_entries.withColumn("call_duration_seconds", col("call_duration_seconds").cast("int")) \
            .withColumn("message_length", col("message_length").cast("int")) \
            .withColumn("signal_strength_dbm", col("signal_strength_dbm").cast("int")) \
            .withColumn("cost", col("cost").cast("double")) \
            .withColumn("call_start_time", to_timestamp("call_start_time")) \
            .withColumn("call_end_time", to_timestamp("call_end_time"))

        # Ensure id values are non null and non positive
        new_changed_entries = new_changed_entries.filter((col("id").isNotNull()) & (col("id") > 0)) 

        #ensure sim_card_number (Starts with '8930' and 19 digits),imsi_number (15 digits) imei_number (15 digits, etc
        new_changed_entries =  new_changed_entries.filter(col("sim_card_number").rlike("^8930\\d{15}$")) \
            .filter(col("imsi_number").rlike("^\\d{15}$")) \
            .filter(col("imei_number").rlike("^\\d{15}$")) \
            .filter(col("country_code").rlike("^\\+(234|1|44).*$")) \
            .filter(col("cell_tower_id").rlike("^CT-\\d{4}$")) \
            .filter(col("location_area_code").rlike("^\\d{3}-\\d{3}$")) 
        
        # Filter by Allowed Values
        new_changed_entries = new_changed_entries.filter(col("call_type").isin("voice", "sms", "data")) \
            .filter(col("service_type").isin("prepaid", "postpaid")) \
            .filter(col("direction").isin("incoming", "outgoing")) \
            .filter(col("network_type").isin("2G", "3G", "4G", "5G")) \
            .filter(col("network_provider").isin("MTN", "Airtel", "Glo", "9mobile")) \
            .filter(col("currency").isin("NGN", "USD")) \
            .filter(col("call_end_time") >= col("call_start_time"))
    
    
        # ========== Merge Section ==========

        if not is_first_run:
            final_df = unchanged_entries_df.unionByName(new_changed_entries)
        else:
            final_df = new_changed_entries

        logger.info(f"Final record count after transformation: {final_df.count()}")
        logger.info("Data Cleaning and Transformation completed successfully.")

        return final_df
        
    except Exception as e:
        logger.error(f" Error during Cleaning and transformation: {e}")
        raise

## define function to write clean dataset to the destination 
def write_data_to_s3(df, output_path: str, format: str = "parquet", mode: str = "overwrite"):
    """Write processed DataFrame to S3."""
    try:
        if df is None:
            logging.warning("No New data to save to s3 ")
        else:

            logger.info(f"Saving DataFrame to {output_path} in {format.upper()} format with mode={mode}...")

            start_time = time.time()

            (df.coalesce(1).write
                .mode(mode)
                .format(format)
                .save(output_path)
            )
            
            logger.info(f"Data successfully saved to {output_path} in {time.time() - start_time:.2f} seconds.")

    except Exception as e:
        logger.error(f"Error while saving data to s3: {e}")
        raise


# define main function
def main():

    try:
        start = time.time()

        env_type,aws_access_key, aws_secret_key, aws_region, jar_path,base_path,source_bucket,target_path = load_environment()
        spark = create_spark_connection(env_type, aws_access_key, aws_secret_key, aws_region, jar_path)
        
        # Get all files uploaded in last 24 hours
        source_files = get_uploaded_files(spark,aws_access_key,aws_secret_key,source_bucket,target_path)
        logging.info(f'The following files were detected :{source_files}')
        
        
        # Read and combine all source files
        df_raw = read_data_from_s3(spark, source_files)
        
        # Transform the data
        df_cleaned = transform_data(spark,df_raw,target_path)
        
        # # Write cleaned data to destination in append mode
        write_data_to_s3(df_cleaned,target_path)

        logger.info("ETL pipeline completed successfully.")
        logger.info(f"Total execution time: {time.time() - start:.2f} seconds")

    except Exception as e:
        logger.critical(f"ETL pipeline failed: {e}")

    except Exception as e:
        logger.critical(f"ETL pipeline failed: {e}")


# Entry Point
if __name__ == "__main__":
    main()