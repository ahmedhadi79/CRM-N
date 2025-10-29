import ast
import os
import time
from datetime import datetime
from datetime import timezone

import jwt
import pandas as pd
import requests
from data_catalog import column_comments
from data_catalog import schemas

# Case1: Execution inside AWS Lambda
if "AWS_LAMBDA_FUNCTION_NAME" in os.environ:
    from api_client import APIClient
    from custom_functions import (
        setup_logger,
        get_actual_dtypes,
        apply_schema,
        raw_load_to_s3,
        get_secret,
    )
# Case2: Local or test execution
else:
    from common.api_client import APIClient
    from common.custom_functions import (
        setup_logger,
        get_actual_dtypes,
        apply_schema,
        raw_load_to_s3,
        get_secret,
    )

logger = setup_logger("GA_ETL")


def get_access_token(service_account_info):
    now = int(time.time())
    jwt_payload = {
        "iss": service_account_info["client_email"],
        "sub": service_account_info["client_email"],
        "aud": service_account_info["token_uri"],
        "iat": now,
        "exp": now + 1800,  # Token valid for half hour
        "scope": "https://www.googleapis.com/auth/analytics.readonly",
    }

    # Sign the JWT with the private key
    signed_jwt = jwt.encode(
        jwt_payload, service_account_info["private_key"], algorithm="RS256"
    )

    # Request an access token from Google's OAuth 2.0 server
    token_response = requests.post(
        service_account_info["token_uri"],
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data={
            "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
            "assertion": signed_jwt,
        },
    )

    token_data = token_response.json()
    if "access_token" in token_data:
        return token_data["access_token"]
    else:
        raise Exception(f"Error obtaining access token: {token_data}")


def ga_response_to_dataframe(data: list, request_body: dict):
    dimension_headers = [header["name"] for header in request_body["dimensions"]]
    metric_headers = [header["name"] for header in request_body.get("metrics", [])]
    headers = dimension_headers + metric_headers

    rows = []
    for row in data:
        # Extract dimension values
        dimension_values = [dim["value"] for dim in row["dimensionValues"]]
        # Extract metric values, if they exist
        metric_values = [metric["value"] for metric in row.get("metricValues", [])]
        # Combine dimension and metric values into a single row
        rows.append(dimension_values + metric_values)

    df = pd.DataFrame(rows, columns=headers)

    # Add meta fields
    df["timestamp_extracted"] = datetime.now(timezone.utc)

    # Add partitioning columns
    if "dateHourMinute" in df.columns:
        df["date"] = (
            df["dateHourMinute"].str.slice(0, 4)
            + "-"
            + df["dateHourMinute"].str.slice(4, 6)
            + "-"
            + df["dateHourMinute"].str.slice(6, 8)
        )
        # Convert to UTC-0
        # https://bb-2.atlassian.net/browse/NM-37987
        df["dateHourMinute"] = (
            pd.to_datetime(df["dateHourMinute"], format="%Y%m%d%H%M")
            .dt.tz_localize("Europe/London", ambiguous=True)
            .dt.tz_convert("UTC")
        )
    elif "date" in df.columns:
        df["date"] = (
            df["date"].str.slice(0, 4)
            + "-"
            + df["date"].str.slice(4, 6)
            + "-"
            + df["date"].str.slice(6, 8)
        )
    else:
        raise Exception(
            "For day partitioning, you have to query at least one of these dimensions ['date', 'dateHourMinute']"
        )

    return df


def request_all_pages(auth_path, login_url, base_url, ga_app_id, json_body):
    logger.info("Calling page1")
    attempt_count = 1
    max_attempts = 3

    while attempt_count <= max_attempts:
        try:
            # Authentication
            ga_client = APIClient(
                auth=f"Bearer {get_access_token(get_secret(auth_path))}",
                base_url=base_url,
                login_url=login_url,
            )

            # Fetch first page
            first_payload = ga_client.post(
                endpoint=f"{ga_app_id}:runReport",
                json_body=json_body,
                clean=True,
            )

            # Check if the payload contains the required data
            payload = first_payload["rows"]
            row_count = first_payload["rowCount"]
            date_range = str(json_body["dateRanges"])

            # If no exceptions occur, break loop
            break

        except KeyError:
            warning_msg = f"runReport returned empty payload.\n\nrequest body: {json_body}\n\nreceived payload: {first_payload}"
            logger.warning(warning_msg)
            attempt_count += 1

            # Sleep for 10 seconds before retrying
            if attempt_count <= max_attempts:
                logger.info(f"Retrying... attempt {attempt_count}")
                time.sleep(10)

            # Raise error if all retries failed
            if attempt_count > max_attempts:
                logger.error("Max retries exceeded. Raising error.")
                raise Exception(warning_msg)

    if row_count >= 100000:
        logger.info(
            f"Found +100,000 ({row_count:,}) records for {date_range} starting pagination.."
        )

        data_list = []
        total_pages = int(first_payload["rowCount"]) // 100000
        if (int(first_payload["rowCount"]) % 100000) != 0:
            total_pages += 1
        page = 1

        # Loop all available pages
        while True:
            data_list.extend(payload)
            page += 1
            json_body["offset"] += 100000

            if page <= total_pages:
                logger.info(f"Calling page{page} out of {total_pages}")
                payload = ga_client.post(
                    endpoint=f"{ga_app_id}:runReport",
                    filter_objects=["rows"],
                    clean=True,
                    json_body=json_body,
                )
            else:
                break
        return data_list
    else:
        # Return page1 only
        logger.info(f"Found ({row_count:,}) records for {date_range}")
        return payload


def lambda_handler(event, context):
    # Stage1: Initializing global variables
    env = os.environ["ENV"]
    ga_view_id = os.environ["GA_VIEW_ID"]
    ga_app_id = os.environ["GA_APP_ID"]
    auth_path = os.environ["AUTH_PATH"]
    wr_write_mode = os.environ["WRANGLER_WRITE_MODE"]
    start_date = os.environ["START_DATE"]
    end_date = os.environ["END_DATE"]
    dimensions_list = ast.literal_eval(os.environ["REPORT_DIMENSIONS"])
    metrics_list = ast.literal_eval(os.environ["REPORT_METRICS"])

    table_name = f"ga_{ga_app_id}_data_view_{ga_view_id}"
    login_url = "https://oauth2.googleapis.com/token"
    base_url = "https://analyticsdata.googleapis.com/v1beta/properties/"

    # Stage2: Initializing GA4 API post request body
    body = {
        "limit": 100000,
        "offset": 0,
        "dateRanges": [{"startDate": "3daysAgo", "endDate": "today"}],
    }

    try:
        datetime.strptime(start_date, "%Y-%m-%d")
        datetime.strptime(end_date, "%Y-%m-%d")
        body["dateRanges"] = [{"startDate": start_date, "endDate": end_date}]
    except ValueError:
        logger.info(
            f"start_date({start_date}) and end_date({end_date}) are not formatted in %Y-%m-%d.\nProceeding with daily batch mode."
        )

    if dimensions_list:
        body["dimensions"] = [{"name": dimension} for dimension in dimensions_list]
    if metrics_list:
        body["metrics"] = [{"name": metric} for metric in metrics_list]

    # Stage3: Calling GA4 reports API endpoint
    json_response = request_all_pages(auth_path, login_url, base_url, ga_app_id, body)

    # Stage4: Generating dataframe with controlled schema
    df = ga_response_to_dataframe(json_response, body)
    if schemas.get(table_name):
        final_schema = schemas
    else:
        final_schema = {table_name: get_actual_dtypes(df)}
    df = apply_schema(df, final_schema[table_name])

    # Stage5: Loading data to S3/Athena
    raw_load_to_s3(
        ingested_df=df,
        table_name=table_name,
        env=env,
        file_type="parquet",
        mode=wr_write_mode,
        column_comments=column_comments,
        schemas=final_schema,
    )

    msg = f"[Success]: Loaded {len(df.index):,} row to datalake_raw.{table_name}"
    logger.info(msg)
    return msg
