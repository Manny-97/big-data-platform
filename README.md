# big-data-platform

### Data Processing


#### Overview
This pipeline is designed to efficiently process large volumes of Parquet files stored in an Amazon S3 bucket. It reads raw data, performs cleaning and transformation, and writes the processed data back to a separate S3 bucket.

#### Key features include:

Initial Full Load: On the first run, the pipeline processes all Parquet files available in the source S3 bucket.

Incremental Loads: On subsequent runs, it processes only new or updated records from the files uploaded in last 24 hours, ensuring no duplicate entries are written to the destination.

Optimized for Scale: By avoiding full reloads, this solution is highly scalable and suitable for handling large datasets.


#### How It Works
##### Initial Run:
Scans all Parquet files in the source S3 bucket.

Applies data cleansing and transformations.

Loads the processed data into the destination S3 bucket.

#### Subsequent Runs:
Detects newly added or updated Parquet files,those one uploaded in last 24 hours.

Processes only the incremental changes.

Merges the clean data into the destination without duplications.