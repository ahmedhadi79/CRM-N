import logging
import os

import awswrangler as wr
import pandas as pd

import config

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def write_as_csv_s3(data: pd.DataFrame, s3_bucket):
    s3_key = "data-lake/sfmc_data.csv"
    s3_path = "s3://" + s3_bucket + "/" + s3_key

    logger.info(f"Shape: {data.shape}")
    logger.info("Uploading to S3 location:  %s", s3_path)

    try:
        res = wr.s3.to_csv(
            df=data,
            path=s3_path,
            index=False,
            dataset=False,
        )
        return res
    except Exception as e:
        logger.error("Failed uploading to S3 location:  %s", s3_path)
        logger.error("Exception occurred:  %s", e)

        exit(1)


def read_athena(sql_path: str, input_database: str) -> pd.DataFrame:
    with open(sql_path, "r") as sql_file:
        sql = sql_file.read()

    logger.info("Reading from Athena... ")
    df = wr.athena.read_sql_query(
        sql=sql,
        database=input_database,
        workgroup="datalake_workgroup",
        ctas_approach=True,
    )

    return df


def lambda_handler(event, context):
    """[summary]
    :param event: [description]
    :type event: [type]
    :param context: [description]
    :type context: [type]
    :return: [description]
    :rtype: [type]
    """

    logger.info("Getting data from Athena...")
    sql_path = config.config["sfmc_data"]["sfmc_data"]
    sfmc_data = read_athena(sql_path, "datalake_raw")
    s3_bucket = os.environ["S3_BUCKET"]

    logger.info("Write as CSV to S3...")
    res_s3_csv = write_as_csv_s3(sfmc_data, s3_bucket)
    logger.info("Finished writing to S3 as CSV...")
    logger.info(res_s3_csv)
