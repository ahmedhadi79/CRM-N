import logging
import os
import sys
from datetime import datetime
from datetime import timezone
from io import BytesIO
from typing import Optional

import awswrangler as wr
import boto3
import pandas as pd
from api_client import APIClient


def setup_logger(
    name: Optional[str] = None,
    level: int = logging.INFO,
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    filename: Optional[str] = None,
) -> logging.Logger:
    """
    Sets up a logger with the specified configuration.

    Parameters:
    - name (Optional[str]): Name of the logger. If None, the root logger is used.
    - level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
    - format (str): Log message format.
    - filename (Optional[str]): If specified, logs will be written to this file. Otherwise, logs are written to stdout.

    Returns:
    - logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if filename:
        handler = logging.FileHandler(filename)
    else:
        handler = logging.StreamHandler(sys.stdout)

    handler.setLevel(level)
    formatter = logging.Formatter(format)
    handler.setFormatter(formatter)

    # To avoid duplicate handlers being added
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger


logger = setup_logger("Lambda")


def make_query(sql):
    logger.info(f"Executing query: {sql}")
    return wr.athena.read_sql_query(
        sql=sql,
        database="datalake_raw",
        workgroup="datalake_workgroup",
        ctas_approach=False,
        keep_files=False,
    )


def get_salesforce_data():
    """Retrieve Salesforce PDF document data from database."""
    sql = """
    SELECT DISTINCT contentdocument_title, linkedentity_casenumber, contentdocument_fileextension, ContentDocument_LatestPublishedVersionId
    FROM salesforce_casenumber_valuation
    WHERE contentdocument_fileextension='pdf'
    """
    return make_query(sql)


def process_and_upload_files(
    df, salesforce_api, s3_client, bucket_name, prefix, delta_mode, sf_version
):
    """Process each file: download from Salesforce and upload to S3."""
    # Get existing files if in delta mode
    existing_files = []
    if delta_mode:
        try:
            available_paths = wr.s3.list_objects(path=f"s3://{bucket_name}/{prefix}")
            existing_files = [s3_path.split("/")[-1] for s3_path in available_paths]
        except ValueError:
            logger.warning(
                f"Can't find files in s3://{bucket_name}/{prefix}, uploading all available files."
            )

    processed_files = []
    for _, row in df.iterrows():
        file_name = f"{row['contentdocument_title']}_{row['linkedentity_casenumber']}.{row['contentdocument_fileextension']}"

        # Skip if file already exists in S3 (delta mode)
        if file_name in existing_files:
            continue

        try:
            # Download file from Salesforce
            logger.info(f"Downloading: {file_name}")
            file_res = salesforce_api.get(
                endpoint=f"services/data/{sf_version}/sobjects/ContentVersion/{row['ContentDocument_LatestPublishedVersionId']}/VersionData/"
            )
            file_obj = BytesIO(file_res.content)

            # Upload file to S3
            s3_key = f"{prefix}{file_name}"
            logger.info(f"Uploading: {s3_key}")
            s3_client.upload_fileobj(file_obj, bucket_name, s3_key)
            logger.info(
                f"File {file_name} uploaded successfully to s3://{bucket_name}/{s3_key}"
            )

            processed_files.append(
                ("success", file_name, f"s3://{bucket_name}/{s3_key}")
            )

        except Exception as e:
            error_msg = str(e)
            processed_files.append(("failed", file_name, error_msg))

    return processed_files


def log_failed_files(bucket_name, prefix, processed_files):
    """
    Log failed files to Athena table in append mode.
    """
    failed_files = [
        (status, file, error_message)
        for status, file, error_message in processed_files
        if status != "success"
    ]
    if failed_files:
        path = f"s3://{bucket_name}/{prefix}failed_files/"
        logger.info(f"Logging failed files {failed_files} to {path}")
        failed_files_df = pd.DataFrame(
            failed_files,
            columns=["status", "file_name", "error_message"],
        )
        failed_files_df["timestamp"] = datetime.now(timezone.utc)
        try:
            wr.s3.to_parquet(
                df=failed_files_df,
                path=path,
                database="datalake_raw",
                table="salesforce_casenumber_valuation_failed_files",
                mode="append",
                dataset=True,
                compression="snappy",
            )
            logger.info("Failed files logged successfully.")
        except Exception:
            logger.exception("Error logging failed files")
            raise


def lambda_handler(event, context):
    """
    Main handler for processing Salesforce valuation files and uploading to S3.
    """
    try:
        # Step1: Load configuration
        prefix = "salesforce-valuation-files/"
        bucket_name = os.environ["S3_RAW"]
        delta_mode = os.environ["DELTA_MODE"]
        sf_version = os.environ["SALESFORCE_API_VERSION"]
        login_url = os.environ["TOKEN_URL"]
        auth = os.environ["SALESFORCE_AUTH_DETAILS"]

        # Step2: Get data from database
        df = get_salesforce_data()
        if df.empty:
            logger.warning("The data list is empty.")
            return "The data list is empty."

        # Step3: Initialize clients
        logger.info("Initializing API handler..")
        salesforce_api = APIClient(login_url=login_url, auth=auth, secrets_manager=True)
        s3_client = boto3.client("s3")

        # Step4: Process files
        processed_files = process_and_upload_files(
            df, salesforce_api, s3_client, bucket_name, prefix, delta_mode, sf_version
        )

        # Step5: Log results
        log_failed_files(
            bucket_name=bucket_name,
            prefix=prefix,
            processed_files=processed_files,
        )

        logger.info(processed_files)
        return processed_files

    except Exception:
        logger.exception("Error in lambda_handler")
        raise
