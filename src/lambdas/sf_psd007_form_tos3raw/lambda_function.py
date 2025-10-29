import logging
import os

import awswrangler as wr
import pandas as pd

import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_as_parquet_s3(data: pd.DataFrame, s3_bucket):
    s3_key = "salesforce_psd007_form_c"
    s3_path = f"s3://{s3_bucket}/{s3_key}/"

    logger.info(f"Shape: {data.shape}")
    logger.info("Uploading Parquet to S3 location: %s", s3_path)

    try:
        res = wr.s3.to_parquet(
            df=data,
            path=s3_path,
            index=False,
            dataset=True,
            mode="overwrite",
            partition_cols=["date"]
        )
        return res
    except Exception as e:
        logger.error("Failed uploading Parquet to S3: %s", s3_path)
        logger.error("Exception occurred: %s", e)
        exit(1)


def read_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()

    logger.info("Reading from Athena...")
    df = wr.athena.read_sql_query(
        sql=sql,
        database=input_database,
        workgroup="datalake_workgroup",
        ctas_approach=False,
    )

    return df


def lambda_handler(event, context):
    """Main Lambda entry point"""

    logger.info("Getting data from Athena...")
    sql_path = config.config["salesforce_psd007_form_c"]["salesforce_psd007_form_c"]
    psd_data = read_athena(sql_path, "datalake_raw")

    s3_bucket = os.environ["S3_RAW"]

    logger.info("Write as Parquet to S3...")
    res_s3_parquet = write_as_parquet_s3(psd_data, s3_bucket)
    logger.info("Finished writing to S3 as Parquet.")
    logger.info(res_s3_parquet)
