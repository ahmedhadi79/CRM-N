import awswrangler as wr
import sys
import pandas as pd
from pandas.errors import ParserError
import boto3
import logging
from awsglue.utils import getResolvedOptions


def initialize_log() -> logging.Logger:
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


logger = initialize_log()


def list_all_csv_files(bucket, base_prefix):
    """List all CSV files recursively under a base prefix"""
    base_path = f"s3://{bucket}/{base_prefix}/"
    all_files = wr.s3.list_objects(base_path)
    return [file for file in all_files if file.endswith(".csv")]


def validate_csv_vs_parquet(csv_path, parquet_path):
    """Validate that CSV and Parquet files match."""
    logger.info(f"Validating CSV {csv_path} against Parquet {parquet_path}")

    try:
        # Read both files
        df_csv = wr.s3.read_csv(csv_path, on_bad_lines="skip")
        df_parquet = wr.s3.read_parquet(parquet_path)

        # Normalize column names (important)
        df_csv.columns = [col.strip().lower() for col in df_csv.columns]
        df_parquet.columns = [col.strip().lower() for col in df_parquet.columns]

        # Check 1: Row count
        if df_csv.shape[0] != df_parquet.shape[0]:
            logger.error(
                f"Row count mismatch! CSV: {df_csv.shape[0]} vs Parquet: {df_parquet.shape[0]}"
            )
            return False

        # Check 2: Column names
        if sorted(df_csv.columns) != sorted(df_parquet.columns):
            logger.error(
                f"Column mismatch!\nCSV Columns: {sorted(df_csv.columns)}\nParquet Columns: {sorted(df_parquet.columns)}"
            )
            return False

        logger.info("Validation passed CSV and Parquet match.")
        return True

    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False


def move_csv_to_archive(csv_path, table_name):
    """Move CSV file to archive location with same date= folder structure"""
    # Extract the relative S3 key and date folder
    parsed = csv_path.replace("s3://", "").split("/")
    bucket = parsed[0]
    key_parts = parsed[1:]

    # Extract date folder from path
    date_folder = [part for part in key_parts if part.startswith("date=")][0]
    file_name = key_parts[-1]

    source_key = "/".join(key_parts)
    target_key = f"{table_name}_archive/{date_folder}/{file_name}"

    logger.info(f"Copying {source_key} to {target_key}")

    s3_client = boto3.client("s3")

    # Copy file
    s3_client.copy_object(
        Bucket=bucket, CopySource={"Bucket": bucket, "Key": source_key}, Key=target_key
    )
    logger.info(f"Copied to archive: s3://{bucket}/{target_key}")

    # Delete original
    s3_client.delete_object(Bucket=bucket, Key=source_key)
    logger.info(f"Deleted original CSV: s3://{bucket}/{source_key}")


failed_csv_files = []  # Global or pass it to the function as needed


def convert_csv_to_parquet(csv_path):
    """Convert a CSV file to Parquet (Snappy) in-place"""
    logger.info(f"Reading CSV: {csv_path}")
    try:
        df = wr.s3.read_csv(csv_path)
    except (ParserError, ValueError, Exception) as e:
        logger.error(f"Failed to read CSV {csv_path}: {e}")
        failed_csv_files.append(csv_path)
        return

    # Convert known timestamp columns
    timestamp_keywords = ["date", "timestamp"]
    for col in df.columns:
        if any(key in col.lower() for key in timestamp_keywords):
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
                logger.info(f"Auto-converted '{col}' to datetime.")
            except Exception as e:
                logger.warning(f"Failed to convert '{col}': {e}")

    parquet_path = csv_path.replace(".csv", ".snappy.parquet")
    logger.info(f"Writing Parquet: {parquet_path}")

    wr.s3.to_parquet(
        df=df, path=parquet_path, dataset=True, mode="overwrite", compression="snappy"
    )
    logger.info("Conversion complete.")

    validation_result = validate_csv_vs_parquet(csv_path, parquet_path)
    if validation_result:
        logger.info("Validation successful.")
    else:
        logger.warning("Validation failed!")


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "BUCKET_NAME",
            "TABLE_NAMES",
            "ARCHIVE_ENABLED",
        ],
    )

    bucket_name = args["BUCKET_NAME"]
    table_name = args["TABLE_NAMES"]
    archive_enabled = args["ARCHIVE_ENABLED"]

    csv_files = list_all_csv_files(bucket_name, table_name)
    for csv_path in csv_files:
        convert_csv_to_parquet(csv_path)
        if archive_enabled:
            move_csv_to_archive(csv_path, table_name)
    
    if failed_csv_files:
        logger.warning("The following CSV files could not be processed:")
        for f in failed_csv_files:
            logger.warning(f)


if __name__ == "__main__":
    main()
