import ast
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import awswrangler as wr
import pandas as pd
from api_client import APIClient
from awsglue.utils import getResolvedOptions
from custom_functions import logger
from requests.exceptions import HTTPError


def initialize_client(args):
    return APIClient(
        login_url=args["LOGIN_URL"],
        auth=args["AUTH_PATH"],
        secrets_manager=True,
    )


def list_month_days():
    today = datetime.now(timezone.utc)
    month_dates = []
    for i in range(30):
        date = today - timedelta(days=i)
        month_dates.append(date.strftime("%d-%m-%Y"))
    return month_dates


def list_stored_dates(bucket_name, table_name):
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
    """This function decides the target_days to be called by APIClient based on three parameters.
    the daily run recommended parameter to be:
    overwrite_mode = False
    delta_mode = True
    target_days = []"""
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


def get_tracking_data(api_instance, start_date, delta_days, activities_to_scan, args):
    """A recursive function that gets all data pages for a target day with a starting delta_days= 1day,
    if the data are more than SFMC API limitation (10,000 rows) the function calls itself
    two times again with half of the timedelta and staks untill API call returns
    values less than the limit"""

    end_date = start_date + timedelta(days=delta_days)
    json_body = {
        "Start": start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "End": end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "activityTypes": activities_to_scan,
    }

    logger.info(
        "Fetching tracking data between "
        + start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        + " and "
        + end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    )

    try:
        sample_page = api_instance.post(
            endpoint="interaction/v1/interactions/journeyhistory/search",
            query="$page=1&$pageSize=1",
            filter_objects=["count", "items"],
            clean=True,
            json_body=json_body,
        )
    except HTTPError as e:
        if e.response.status_code == 401:
            logger.warning("401 Unauthorized error, reinitializing API client.")
            api_instance = initialize_client(args)
            return get_tracking_data(
                api_instance, start_date, delta_days, activities_to_scan, args
            )
        else:
            raise e

    if sample_page.get("count", 0) == 10000:
        logger.info(
            "Found +10,000 records from "
            + start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            + " to "
            + end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            + " splitting duration and request again.."
        )

        delta_days = delta_days / 2
        mid_date = start_date + timedelta(days=delta_days)
        # Call the API again for the first half
        first_half = get_tracking_data(
            api_instance, start_date, delta_days, activities_to_scan, args
        )
        # Call the API again for the second half
        second_half = get_tracking_data(
            api_instance, mid_date, delta_days, activities_to_scan, args
        )
        # Combine the results
        all_pages = first_half + second_half
    elif sample_page.get("count", 0) == 0:
        logger.info(
            "Found no data from "
            + start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            + " to "
            + end_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        )
        # Function exit
        return []
    else:
        # Recursion exit
        all_pages = api_instance.post(
            endpoint="interaction/v1/interactions/journeyhistory/search",
            query="$page=1&$pageSize=10000",
            filter_objects=["items"],
            clean=True,
            json_body=json_body,
        )

        logger.info("Received: " + "{:,}".format(len(all_pages)) + " Records.")

    return all_pages


def save_to_parquet_s3(all_pages, bucket_name, table_name, day):
    df = pd.DataFrame(all_pages)
    df = df.drop(columns=["result"])

    abs_path = f"s3://{bucket_name}/{table_name}/{day}.parquet"

    logger.info(
        "Uploading parquet file to "
        + abs_path
        + " \nTotal Records: "
        + "{:,}".format(len(df))
    )

    wr.s3.to_parquet(df, abs_path)
    logger.info("[Sucess]: Uploaded to " + abs_path)


def main():
    logger.info("Initializing API handler..")

    # Stage1: Initializing environment variables
    args = getResolvedOptions(
        sys.argv,
        [
            "ENV",
            "OVERWRITE_MODE",
            "DELTA_MODE",
            "TARGET_DAYS",
            "ACTIVITIES_TO_SCAN",
            "LOGIN_URL",
            "AUTH_PATH",
        ],
    )

    overwrite_mode = ast.literal_eval(args["OVERWRITE_MODE"])
    delta_mode = ast.literal_eval(args["DELTA_MODE"])
    target_days = ast.literal_eval(args["TARGET_DAYS"])
    activities_to_scan = ast.literal_eval(args["ACTIVITIES_TO_SCAN"])
    env = args["ENV"]
    bucket_name = f"bb2-{env}-datalake-raw"
    table_name = "mc_activities_sent"

    # Stage2: Initializing client and OAuth2 authintication
    mc_api = initialize_client(args)

    # Stage3: Aquiring the list of days to be saved
    target_days = get_target_days(
        overwrite_mode, delta_mode, target_days, bucket_name, table_name
    )

    logger.info(
        f"Found {str(len(target_days))} day/s to be ingested {str(target_days)}"
    )

    logger.info(f"Activity types to scan: {str(activities_to_scan)}")

    for day in target_days:
        start_date = datetime.strptime(day, "%d-%m-%Y")
        delta_days = 1
        logger.info(f"Calling tracking data for {day}")

        # Stage4: Recursively call all available tracking events for each day
        one_day_data = get_tracking_data(
            mc_api, start_date, delta_days, activities_to_scan, args
        )

        # Stage5: Save data to s3 in parquet format partitioned by day
        if one_day_data:
            save_to_parquet_s3(one_day_data, bucket_name, table_name, day)

    return f"[Success] Saved days: {str(target_days)}"


if __name__ == "__main__":
    main()
