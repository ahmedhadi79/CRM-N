import ast
import logging
import os
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Optional

import awswrangler as wr
import pandas as pd
from soap_client import SOAPClient


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


logger = setup_logger("mc_factory_soap_to_s3_raw")


def list_month_days():
    """
    Generate a list of date strings for the last 30 days.

    Returns:
        list: List of date strings formatted as "%d-%m-%Y".
    """
    today = datetime.now(timezone.utc)
    month_dates = [today - timedelta(days=i) for i in range(30)]
    return [date.strftime("%d-%m-%Y") for date in month_dates]


def list_stored_dates(bucket_name, table_name):
    """
    List date strings already stored in the specified S3 path.

    Args:
        bucket_name (str): S3 bucket name.
        table_name (str): Path within the S3 bucket.

    Returns:
        list: List of date strings.
    """
    table_path = f"s3://{bucket_name}/{table_name}/"
    available_dates = []
    available_paths = wr.s3.list_objects(
        table_path,
        ignore_empty=True,
    )
    for s3_path in available_paths:
        date_string = s3_path.split("/")[-1].split(".")[0]
        available_dates.append(date_string)
    return available_dates


def get_target_days(overwrite_mode, delta_mode, target_days, bucket_name, table_name):
    """
    Decide target days based on overwrite and delta modes.

    Args:
        overwrite_mode (bool): Whether to overwrite existing data.
        delta_mode (bool): Whether to fetch only new data.
        target_days (list): List of specific days to process.
        bucket_name (str): S3 bucket name.
        table_name (str): Path within the S3 bucket.

    Returns:
        list: List of date strings.
    """
    if overwrite_mode:
        if delta_mode and target_days:
            raise SyntaxError(
                "If <DELTA_MODE=True>, the argument <TARGET_DAYS> should be an empty list."
            )
        elif delta_mode and not target_days:
            # Overwrite all valid dates even if they are stored before
            return list_month_days()
        else:
            # Overwrite only dates specified in TARGET_DATES
            return target_days
    else:
        if delta_mode and target_days:
            raise SyntaxError(
                "If <DELTA_MODE=True>, the argument <TARGET_DAYS> should be an empty list."
            )
        elif delta_mode and not target_days:
            # List the last month days excluding the days already stored before, except the last day stored to be overwritten.
            month_dates = list_month_days()
            available_dates = list_stored_dates(bucket_name, table_name)
            target_days = [date for date in month_dates if date not in available_dates]

            # Getting latest saved date to be overwritten
            available_dates_objects = [
                datetime.strptime(date, "%d-%m-%Y") for date in available_dates
            ]
            if available_dates_objects:
                latest_date = max(available_dates_objects).strftime("%d-%m-%Y")
                target_days.append(latest_date)

            return target_days
        else:
            # Pop the already stored days even if they are listed in TARGET_DATES
            available_dates = list_stored_dates(bucket_name, table_name)
            return [date for date in target_days if date not in available_dates]


def get_tracking_data(
    api_instance, start_date, object, properties, filterOperator, filterProperty
):
    """
    Fetch tracking data for a specific day.

    Args:
        api_instance (SOAPClient): Instance of SOAPClient for API requests.
        start_date (datetime): Start date for data retrieval.
        object_type (str): Type of object to retrieve.
        properties (list): List of properties to fetch.
        filter_operator (str): Filter operator for query.
        filter_property (str): Filter property for query.

    Returns:
        list: List of tracking data pages.
    """

    end_date = start_date + timedelta(days=1)
    filterValues = [
        start_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
        end_date.strftime("%Y-%m-%dT%H:%M:%SZ"),
    ]

    logger.info(
        "Fetching tracking data between "
        + start_date.strftime("%Y-%m-%d")
        + " and "
        + end_date.strftime("%Y-%m-%d")
    )

    all_pages = api_instance.request(
        object_type=object,
        properties=properties,
        filter_operator=filterOperator,
        filter_property=filterProperty,
        filter_values=filterValues,
    )

    return all_pages


def save_to_parquet_s3(all_pages, bucket_name, table_name, day):
    """
    Save data pages to S3 in Parquet format.

    Args:
        all_pages (list): List of data pages to save.
        bucket_name (str): S3 bucket name.
        table_name (str): Path within the S3 bucket.
        day (str): Date string to use for file partitioning.
    """
    df = pd.DataFrame(all_pages)

    abs_path = f"s3://{bucket_name}/{table_name}/{day}.parquet"

    logger.info("Uploading parquet file to " + abs_path)
    wr.s3.to_parquet(df, abs_path)
    logger.info("[Sucess]: Uploaded to " + abs_path)


def lambda_handler(event, context):
    logger.info("Initializing API handler..")

    # Stage1: Initializing environment variables
    env = os.environ["ENV"]
    table_name = os.environ["TABLE_NAME"]
    overwrite_mode = ast.literal_eval(os.environ["OVERWRITE_MODE"])
    delta_mode = ast.literal_eval(os.environ["DELTA_MODE"])
    target_days = ast.literal_eval(os.environ["TARGET_DAYS"])

    object = os.environ["OBJECT_NAME"]
    properties = ast.literal_eval(os.environ["PROPERTIES"])
    filterOperator = os.environ["FILTER_OPERATOR"]
    filterProperty = os.environ["FILTER_PROPERTY"]

    bucket_name = f"bb2-{env}-datalake-raw"

    # Stage2: Initializing client and OAuth2 authintication
    mc_soap_api = SOAPClient(
        login_url=os.environ["LOGIN_URL"],
        auth=os.environ["AUTH_PATH"],
        secrets_manager=True,
    )

    # Stage3: Aquiring the list of days to be saved
    target_days = get_target_days(
        overwrite_mode, delta_mode, target_days, bucket_name, table_name
    )

    logger.info(
        f"Found {str(len(target_days))} day/s to be ingested {str(target_days)}"
    )

    for day in target_days:
        day_object = datetime.strptime(day, "%d-%m-%Y")
        logger.info(f"Calling tracking data for {day}")

        # Stage4: Recursively call all available tracking events for this day
        one_day_data = get_tracking_data(
            mc_soap_api, day_object, object, properties, filterOperator, filterProperty
        )

        # Stage5: Save data to s3 in parquet format partitioned by day
        if one_day_data:
            save_to_parquet_s3(one_day_data, bucket_name, table_name, day)

    return f"[Success] Saved days: {str(target_days)}"
