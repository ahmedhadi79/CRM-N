import logging
import os
import sys
from typing import Optional

import awswrangler as wr
import pandas as pd


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


logger = setup_logger("mc_reporting_first_funded_acc")


def write_as_csv_to_s3(data: pd.DataFrame, s3_bucket: str) -> dict:
    """
    Write a DataFrame to a CSV file in an S3 bucket.

    :param data: DataFrame to be written to CSV
    :param s3_bucket: Name of the S3 bucket
    :return: Result of the S3 write operation
    :rtype: dict
    """
    s3_key = "data-lake/sfmc_first_funded_acc_data.csv"
    s3_path = f"s3://{s3_bucket}/{s3_key}"

    logger.info(f"Data shape: {data.shape}")
    logger.info("Uploading to S3 location: %s", s3_path)

    try:
        result = wr.s3.to_csv(df=data, path=s3_path, index=False, dataset=False)
        return result
    except Exception as e:
        logger.error("Failed uploading to S3 location: %s", s3_path)
        logger.error("Exception occurred: %s", e)
        raise


def read_from_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    """
    Execute an SQL query in Athena and return the results as a DataFrame.

    :param sql_path: Path to the SQL file
    :param input_database: Athena database name
    :return: DataFrame containing query results
    :rtype: pd.DataFrame
    """
    with open(sql_path, "r") as sql_file:
        sql_query = sql_file.read()

    logger.info("Reading from Athena...")
    df = wr.athena.read_sql_query(
        sql=sql_query,
        database=input_database,
        workgroup="datalake_workgroup",
        ctas_approach=False,
    )

    return df


def lambda_handler(event, context):
    """
    Lambda function handler to read data from Athena and write it as a CSV to S3.

    :param event: Lambda event object
    :param context: Lambda context object
    :return: None
    """
    try:
        logger.info("Getting data from Athena...")
        sfmc_data = read_from_athena("query.sql", "datalake_raw")

        s3_bucket = os.environ["S3_BUCKET"]
        logger.info("Write as CSV to S3...")
        res_s3_csv = write_as_csv_to_s3(sfmc_data, s3_bucket)

        logger.info("Finished writing to S3 as CSV...")
        logger.info(res_s3_csv)
    except Exception as e:
        logger.error("An error occurred in the lambda handler: %s", e)
        raise
