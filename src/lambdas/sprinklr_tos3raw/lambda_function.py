import json
import os
import time
import urllib.parse
from datetime import datetime
from datetime import timezone

import awswrangler as wr
import boto3
import data_catalog
import pandas as pd
import requests
from api_client import APIClient
from custom_functions import apply_schema
from custom_functions import get_secret
from custom_functions import logger
from flatten_json import flatten


table_name = "sprinklr_data_view_1"

logger.info("Saving results..")


def get_sprinklr_token(payload):
    try:
        # URL encode the refresh token
        encoded_refresh_token = urllib.parse.quote(payload["refresh_token"])

        # Construct the URL for the API request
        url = (
            "https://api2.sprinklr.com/prod3/oauth/token?"
            f"client_id={payload['client_id']}&"
            f"client_secret={payload['client_secret']}&"
            "redirect_uri=https://www.sprinklr.com/&"
            f"grant_type=refresh_token&"
            f"refresh_token={encoded_refresh_token}"
        )
        headers = {
            "Content-Type": "application/x-www-form-urlencoded",
        }

        # Make the API request to get the token
        response = requests.post(url, headers=headers)

        # Check if the response was successful
        if response.status_code == 200:
            token_data = response.json()

            # Log new tokens
            logger.info("New access token and refresh token have been generated.")

            return token_data
        else:
            # Handle non-successful response
            logger.error(
                f"Failed to get token: {response.status_code} - {response.text}, "
                "Please need to enter the new refresh token and access token by human in the AWS secret manager"
            )
            return None

    except KeyError as e:
        # Catch missing keys in the payload
        logger.error(f"Missing required field in payload: {e}")
        return None

    except requests.exceptions.RequestException as e:
        # Catch any other request-related errors
        logger.error(f"An error occurred during the token request: {e}")
        return None

    except Exception as e:
        # Catch any other unforeseen errors
        logger.error(f"An unexpected error occurred: {e}")
        return None


def build_payload(payload_path, start_epoch, end_epoch, page):
    """Load and Update the payload with startTime, endTime, and page."""
    with open(payload_path, "r") as file:
        payload = json.load(file)
    payload["startTime"] = start_epoch
    payload["endTime"] = end_epoch
    payload["page"] = page
    payload["jsonResponse"] = True
    payload["pageSize"] = 300
    payload["timeField"] = "date"
    return payload


def get_sprinklr_paginated_data(sprinklr_api, start_epoch, end_epoch):
    all_data = []
    page = 1
    while True:
        # Build the request body for the current page
        json_body = build_payload("payload.json", start_epoch, end_epoch, page - 1)

        try:
            logger.info(f"Retreiving page {page}")
            # Make the API request
            response = sprinklr_api.post(
                endpoint="prod3/api/v2/reports/query",
                json_body=json_body,
                clean="true",
                filter_objects=["data"],
            )

            # Check if the response is None or if it doesn't have the 'data' key
            if response is None:
                logger.info(f"Received no response on page {page}. Ending pagination.")
                break
            if "data" not in response:
                logger.info(
                    f"No data found, ending pagination on page {page}. Response: {response}"
                )
                break

            # Append the data to the all_data list
            all_data.extend(response["data"])

            # Log the page number
            logger.info(
                f"Page {page} retrieved, {len(response['data'])} records found."
            )

            # Increment the page for the next request
            page += 1

            # Introduce a short delay if needed to avoid rate limits
            time.sleep(1)

        except Exception as e:
            logger.info(f"Error occurred while fetching page {page}: {e}")
            break

    return all_data


def make_query(sql):
    logger.info(f"Executing query: {sql}")
    return wr.athena.read_sql_query(
        sql=sql,
        database="datalake_raw",
        workgroup="datalake_workgroup",
        ctas_approach=False,
        keep_files=False,
    )


def get_start_epoch_from_athena(table_name: str):
    """
    Fetches the latest creation_date from the Athena table.
    If the table does not exist, returns None.
    """

    # Query Athena for the latest timestamp_extracted
    query = f"""
    SELECT MAX(message_sn_created_time_2)
    FROM datalake_raw.{table_name}
    """
    logger.info("Executing Athena query to fetch start_time")
    result = make_query(query)

    if not result.empty:
        latest_creation_date = result["_col0"].iloc[0]
        logger.info(f"Latest created_date from Athena: {latest_creation_date}")

        if not isinstance(latest_creation_date, pd.Timestamp):
            try:
                latest_creation_date = pd.Timestamp(latest_creation_date)

            except Exception as e:
                logger.error(f"Error parsing created_date: {e}")
                raise ValueError(
                    f"Invalid datetime format for created_date: {latest_creation_date}"
                )

        return int(latest_creation_date.timestamp()) * 1000


def lambda_handler(event, context):
    try:
        # Step1: Initialize start and end timestamps
        start_epoch = (
            int(pd.Timestamp(event.get("start_date")).timestamp()) * 1000
            if event.get("start_date")
            else get_start_epoch_from_athena(table_name)
        )
        end_epoch = (
            int(pd.Timestamp(event.get("end_date")).timestamp()) * 1000
            if event.get("end_date")
            else int(datetime.now().timestamp()) * 1000
        )
        env = os.environ["ENV"]
        target_bucket_name = f"bb2-{env}-datalake-raw"
        path = f"s3://{target_bucket_name}/{table_name}/"

        # Step2: Get credentials while saving the rotated refresh token for the next run.
        secret_payload = get_secret(os.environ["AUTH_PATH"])
        new_payload = get_sprinklr_token(secret_payload)
        secret_payload.update(
            {
                "access_token": new_payload["access_token"],
                "refresh_token": new_payload["refresh_token"],
            }
        )
        client = boto3.client("secretsmanager")
        # Update the secret
        client.update_secret(
            SecretId=os.environ["AUTH_PATH"],
            SecretString=json.dumps(secret_payload),
        )

        # Step3: Initialize API Client
        logger.info("Initializing API handler..")
        sprinklr_api = APIClient(
            auth=f"Bearer {secret_payload['access_token']}",
            base_url="https://api2.sprinklr.com/",
            secrets_manager=False,
        )

        # Step4: Retrieving data
        all_data = get_sprinklr_paginated_data(sprinklr_api, start_epoch, end_epoch)
        logger.info(f"Total records retrieved: {len(all_data)}")

        # Step5: Processing data
        if all_data:
            all_data = [flatten(entry) for entry in all_data]
            df = pd.DataFrame(all_data)
            df["timestamp_extracted"] = datetime.now(timezone.utc)

            # Function to convert epoch to human-readable timestamp
            df["MESSAGE_SN_CREATED_TIME_2"] = df["MESSAGE_SN_CREATED_TIME_2"].apply(
                lambda x: datetime.utcfromtimestamp(int(x) / 1000).strftime(
                    "%Y-%m-%d %H:%M:%S"
                )
            )

            # Get the date part for data partitioning
            df["date"] = df["MESSAGE_SN_CREATED_TIME_2"].str.slice(0, 10)

            df = apply_schema(df, data_catalog.schemas[table_name])

            # Step6: Uploading
            logger.info(
                f"[load_to_s3]: Uploading to S3 path s3://{target_bucket_name}/{table_name}/"
            )
            logger.info("Dataframe shape:  %s", df.shape)

            wr.s3.to_parquet(
                df=df,
                path=path,
                database="datalake_raw",
                table=table_name,
                partition_cols=["date"],
                mode="append",
                max_rows_by_file=400000,
                use_threads=True,
                index=False,
                dataset=True,
                schema_evolution=True,
                compression="snappy",
                dtype=data_catalog.schemas[table_name],
                glue_table_settings=wr.typing.GlueTableSettings(
                    columns_comments=data_catalog.column_comments[table_name]
                ),
            )
            res = f"[Success] Uploaded the period ({start_epoch},{end_epoch}) to {path} shape {df.shape}"
            logger.info(res)

        else:
            res = f"No received data for period [{start_epoch}, {end_epoch}]"
            logger.warning(res)
    except Exception as e:
        res = f"Error in Lambda handler: {e}"
        logger.error(res)
        raise

    finally:
        return res
