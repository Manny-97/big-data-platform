import os
import time
import logging
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import col, when, length, lit, to_timestamp, greatest, least
from datetime import datetime

# Create logs directory if not exists
os.makedirs("logs", exist_ok=True)
log_filename = datetime.now().strftime("logs/log_%Y-%m-%d_%H-%M-%S.log")
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    handlers=[logging.FileHandler(log_filename), logging.StreamHandler()],
)

logger = logging.getLogger(__name__)

def load_environment():

    """Load and return necessary environment variables."""
    load_dotenv()
    env_type = os.getenv("ENVIRONMENT", None)  # None means production
    aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_REGION")
    jar_path = os.getenv("SPARK_JAR_PATH")
    return env_type, aws_access_key, aws_secret_key, aws_region, jar_path


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
                .set("spark.driver.extraClassPath", f"{jar_path}hadoop-aws-3.3.4.jar,{jar_path}aws-java-sdk-bundle-1.12.353.jar").set("spark.hadoop.fs.s3a.fast.upload", "true") \
                .set("spark.hadoop.fs.s3a.multipart.size", "104857600") \
                .set("spark.hadoop.fs.s3a.connection.maximum", "7") \
                .set("spark.hadoop.fs.s3a.threads.max", "7") \
                .set("spark.driver.memory", "8g") \

            master = "local[6]"
        else:
            master = None  # EMR cluster

        conf.set("spark.sql.adaptive.enabled", "true") \
            .set("spark.sql.adaptive.skewJoin.enabled", "true")
        

        # Define default builder
        builder = SparkSession.builder.appName("Telco_ETL_Job").config(conf=conf)

        if master:
            builder = builder.master(master)

        spark = builder.getOrCreate()
        logger.info("Spark session created.")
        return spark

    except Exception as e:
        logger.error(f" Error creating Spark session: {e}")
        raise


def read_data_from_s3(spark, s3_path):
    """Read dataset from S3 using Spark."""
    try:
        start = time.time()
        df = spark.read.parquet(s3_path)
        logger.info(f"Read data from S3: {s3_path} in {time.time() - start:.2f} seconds.")
        return df
    except Exception as e:
        logger.error(f"Failed to read data from S3: {e}")
        raise


def transform_data(df):

    """Apply transformation and data quality rules."""
    try:
        # casting column to appropriate type
        df = df.withColumn("call_duration_seconds", col("call_duration_seconds").cast("int")) \
               .withColumn("message_length", col("message_length").cast("int")) \
               .withColumn("signal_strength_dbm", col("signal_strength_dbm").cast("int")) \
               .withColumn("cost", col("cost").cast("double")) \
               .withColumn("call_start_time", to_timestamp("call_start_time")) \
               .withColumn("call_end_time", to_timestamp("call_end_time"))

        # Ensure id values are non null and non positive
        df = df.filter((col("id").isNotNull()) & (col("id") > 0)) 

        #ensure sim_card_number (Starts with '8930' and 19 digits),imsi_number (15 digits) imei_number (15 digits, etc
        df =  df.filter(col("sim_card_number").rlike("^8930\\d{15}$")) \
               .filter(col("imsi_number").rlike("^\\d{15}$")) \
               .filter(col("imei_number").rlike("^\\d{15}$")) \
               .filter(col("country_code").rlike("^\\+(234|1|44).*$")) \
               .filter(col("cell_tower_id").rlike("^CT-\\d{4}$")) \
               .filter(col("location_area_code").rlike("^\\d{3}-\\d{3}$")) 
        
        # Filter by Allowed Values
        df = df.filter(col("call_type").isin("voice", "sms", "data")) \
               .filter(col("service_type").isin("prepaid", "postpaid")) \
               .filter(col("direction").isin("incoming", "outgoing")) \
               .filter(col("network_type").isin("2G", "3G", "4G", "5G")) \
               .filter(col("network_provider").isin("MTN", "Airtel", "Glo", "9mobile")) \
               .filter(col("currency").isin("NGN", "USD")) \
               .filter(col("call_end_time") >= col("call_start_time"))
        
        print(f'Remaning records after transformation are: {df.count()}')

        logger.info("Data transformation complete.")
        return df
    except Exception as e:
        logger.error(f" Error during transformation: {e}")
        raise

# Entry Point
if __name__ == "__main__":

    try:
        start = time.time()

        env_type, aws_access_key, aws_secret_key, aws_region, jar_path = load_environment()
        spark = create_spark_connection(env_type, aws_access_key, aws_secret_key, aws_region, jar_path)
        
        source_path = "s3a://telecom-synthetic-data/telco_data_5m.parquet"
        output_path = "s3a://processed-teleco-data/telco_data_cleaned.parquet"
        
        df_raw = read_data_from_s3(spark, source_path)
        df_cleaned = transform_data(df_raw)

        logger.info("ETL pipeline completed successfully.")
        logger.info(f"It tooks {time.time() - start:.2f} to complete the execution of the pipeline.")

    except Exception as e:
        logger.critical(f"ETL pipeline failed: {e}")
