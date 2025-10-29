import os
from datetime import datetime, timezone
import awswrangler as wr
import pandas as pd
from data_catalog import column_comments, schemas

# Environment-based imports
if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    from api_client import APIClient
    from custom_functions import setup_logger, raw_load_to_s3
else:
    from common.api_client import APIClient
    from common.custom_functions import setup_logger, raw_load_to_s3

# Constants
TABLE_NAME = "nice_skills_summary"
ATHENA_DB = "datalake_raw"
ATHENA_WORKGROUP = "datalake_workgroup"
ATHENA_TABLE = f"{ATHENA_DB}.{TABLE_NAME}"

logger = setup_logger(TABLE_NAME)


def get_date_range(event):
    """Extracts start and end datetime from event or Athena."""
    start_str = event.get("start_date")
    end_str = event.get("end_date")

    if start_str and end_str:
        logger.info(f"Using provided event date range: {start_str} to {end_str}")
        start = datetime.strptime(start_str, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        )
        end = datetime.strptime(end_str, "%Y-%m-%d %H:%M:%S.%f").replace(
            tzinfo=timezone.utc
        )
    else:
        logger.info("No date range provided. Fetching max timestamp from Athena.")
        max_ts_df = wr.athena.read_sql_query(
            sql=f"SELECT MAX(timestamp_extracted) as max_timestamp FROM {ATHENA_TABLE}",
            database=ATHENA_DB,
            workgroup=ATHENA_WORKGROUP,
            ctas_approach=False,
        )
        max_timestamp = max_ts_df.loc[0, "max_timestamp"]
        start = pd.to_datetime(max_timestamp).tz_localize("UTC")
        end = datetime.now(timezone.utc)

    logger.info(f"Final date range: {start} → {end}")
    return start, end


def fetch_nice_data(api_client, start, end):
    """Fetch data from NICE API."""
    start_fmt = start.strftime("%Y-%m-%dT%H:%M:%S.%f")
    end_fmt = end.strftime("%Y-%m-%dT%H:%M:%S.%f")

    logger.info(f"Calling NICE API for data between {start_fmt} and {end_fmt}")
    df = api_client.get(
        endpoint=f"skills/summary?startDate={start_fmt}&endDate={end_fmt}",
        clean=True,
        filter_objects=["skillSummaries"],
        flatten=True,
        df=True,
    )

    if df is None or df.empty:
        logger.warning(
            f"No data returned for range {start_fmt} to {end_fmt}. Proceeding with empty DataFrame."
        )

    return df


def lambda_handler(event, context):
    logger.info("Lambda execution started")

    start, end = get_date_range(event)

    # Initialize NICE API client
    nice_api = APIClient(
        login_url="https://api-uk1.niceincontact.com/auth/token",
        auth="sls/data/niceAuthDetails",
        base_url="https://api-l35.niceincontact.com/incontactapi/services/v31.0/",
        secrets_manager=True,
    )

    df = fetch_nice_data(nice_api, start, end)

    logger.info(f"Rows to upload: {len(df)}")

    result = raw_load_to_s3(
        ingested_df=df,
        table_name=TABLE_NAME,
        column_comments=column_comments,
        schemas=schemas,
        env=os.environ["ENV"],
        file_type="parquet",
        mode="append",
    )

    logger.info("Data successfully written to S3.")
    return result
