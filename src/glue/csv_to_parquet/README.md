# 📦 CSV to Parquet Converter with Validation and Archiving

This script is designed to be used in an AWS Glue Python Shell Job. It reads CSV files from an S3 path, converts them to Parquet (Snappy compression), validates the conversion, and optionally moves the original CSVs to an archive location.

## ✅ Features
	•	Converts .csv files to .parquet format (with Snappy compression)
	•	Automatically detects and converts date/time columns
	•	Validates row count and column names between CSV and Parquet
	•	Archives original CSV files if enabled

## 📁 S3 Structure
	•	Input path: s3://<bucket>/<table_name>/date=YYYYMMDD/<file>.csv
	•	Output path: s3://<bucket>/<table_name>/date=YYYYMMDD/<file>.snappy.parquet
	•	Archive path: s3://<bucket>/<table_name>_archive/date=YYYYMMDD/<file>.csv

## 🧪 Example Job Arguments
    --BUCKET_NAME bb2-sandbox-datalake-raw
    --TABLE_NAMES salesforce_account
    --ARCHIVE_ENABLED true

## 🚀 Execution Steps
	1.	Initialize logger
	2.	List all .csv files under the given S3 path
	3.	For each file:
	•	Convert to .snappy.parquet
	•	Attempt to auto-convert timestamp columns
	•	Validate structure (row count & columns)
	•	Archive CSV if enabled

⸻

## 🧱 Dependencies
	•	awswrangler
	•	boto3
	•	pandas
	•	aws-glue-utils (built-in for Glue Python shell)

⸻

## 📝 Notes
	•	Ensure proper IAM permissions are in place for read/write/delete on S3.
	•	This script expects a folder structure that includes date= partitions.
	•	It’s best to test with sample files before running on production data.
